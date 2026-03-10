# 在 Ubuntu 24.04.2 LTS 上部署 poly2

本文说明如何在 Ubuntu 24.04.2 LTS 服务器上部署 poly2 策略交易引擎。

---

## 一、环境准备

### 1. 安装 Rust

```bash
# 安装 curl（若未安装）
sudo apt update
sudo apt install -y curl

# 安装 rustup（Rust 官方推荐方式）
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y

# 加载环境变量
source "$HOME/.cargo/env"

# 验证（需要 1.75+）
rustc --version
cargo --version
```

### 2. 安装构建依赖（可选，仅当编译报错时）

```bash
sudo apt install -y pkg-config build-essential libssl-dev
```

---

## 二、部署项目

### 方式 A：从 git 克隆（推荐）

```bash
# 选择部署目录，例如 /opt
sudo mkdir -p /opt
sudo chown "$USER:$USER" /opt
cd /opt

# 克隆仓库（替换为你的仓库地址）
git clone https://github.com/你的用户名/poly2.git
cd poly2
```

### 方式 B：从本地上传

在本地打包（不含 target）：

```bash
# 在 Windows 项目目录
tar --exclude=target --exclude=.git -cvf poly2.tar .
# 或用 zip
# 上传 poly2.tar 到服务器
```

在服务器解压：

```bash
sudo mkdir -p /opt/poly2
sudo chown "$USER:$USER" /opt/poly2
cd /opt/poly2
# 上传后
tar -xvf /path/to/poly2.tar
```

---

## 三、编译与配置

### 1. 编译 release

```bash
cd /opt/poly2   # 或你的项目根目录
cargo build --release
```

二进制位置：`target/release/poly2`。

### 2. 配置文件

- **配置文件**：`config/default.yaml`  
  按需修改策略、风控、execution URL 等。
- **环境与密钥**：`src/.env`  
  必须配置 CLOB、RPC 等，例如：

```bash
# 编辑 src/.env（不要提交到 git）
nano src/.env
```

至少设置：

```env
CLOB_HTTP_URL=https://clob.polymarket.com/
CLOB_WS_URL=wss://ws-subscriptions-clob.polymarket.com/ws
RPC_URL=https://polygon-rpc.com
# USDC_CONTRACT_ADDRESS=...  # 健康检查用，按需
# FETCH_INTERVAL=60          # 主程序市场扫描间隔（秒），默认 60，按需修改
```

### 3. 验证

**健康检查：**

```bash
cd /opt/poly2
./target/release/poly2 healthcheck
```

期望看到 `healthcheck_passed=true`。

**扫描套利候选：**

```bash
./target/release/poly2 scan-arb
```

---

## 四、以 systemd 服务长期运行（推荐）

主程序**自带循环**：默认运行时会按 **`FETCH_INTERVAL`**（在 `src/.env` 中配置，单位秒，默认 60）定期扫描市场并执行策略，无需再用 cron 定时拉起。只需用 systemd 保持一个常驻进程即可。

### 1. 创建服务文件

```bash
sudo nano /etc/systemd/system/poly2.service
```

内容示例（按实际路径改）：

```ini
[Unit]
Description=Poly2 Strategy Trading Engine
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=你的运行用户
Group=你的运行用户
WorkingDirectory=/opt/poly2
ExecStart=/opt/poly2/target/release/poly2
Restart=on-failure
RestartSec=10
StandardOutput=journal
StandardError=journal
Environment=RUST_BACKTRACE=1

[Install]
WantedBy=multi-user.target
```

将 `你的运行用户`、`/opt/poly2` 换成实际用户和项目路径。

### 2. 启动与开机自启

```bash
sudo systemctl daemon-reload
sudo systemctl enable poly2
sudo systemctl start poly2
sudo systemctl status poly2
```

查看日志：

```bash
journalctl -u poly2 -f
```

---

## 五、仅定时跑 healthcheck / scan-arb（可选，仅监控场景）

**正常部署**：用 systemd 跑主程序，扫描间隔由 `src/.env` 里的 **`FETCH_INTERVAL`** 控制即可，不需要 cron。

只有在**不跑主引擎**、只希望定时执行健康检查或套利扫描（例如单独做监控/告警）时，才用 cron：

```bash
crontab -e
```

示例：每 5 分钟健康检查，每 15 分钟扫描套利并写日志：

```cron
*/5 * * * * /opt/poly2/target/release/poly2 healthcheck >> /var/log/poly2-healthcheck.log 2>&1
*/15 * * * * /opt/poly2/target/release/poly2 scan-arb >> /var/log/poly2-scan-arb.log 2>&1
```

需保证对 `/var/log/poly2-*.log` 有写权限（或改为 `$HOME/logs/` 并先 `mkdir`）。

---

## 六、注意事项

1. **工作目录**：程序会从**当前工作目录**读取 `config/default.yaml` 和 `src/.env`，因此：
   - 用 systemd 时务必设置 `WorkingDirectory=/opt/poly2`（或你的项目根）。
   - 用 cron 时在任务里先 `cd /opt/poly2` 或在脚本里 `cd` 再执行 `./target/release/poly2`。
2. **安全**：`src/.env` 含 API Key/私钥，权限建议 `chmod 600 src/.env`，不要提交到 git。
3. **网络**：服务器需能访问 Polymarket CLOB、RPC 等；若有防火墙，放行相应出站 HTTPS/WSS。
4. **实盘**：当前为工程骨架，建议先在模拟/小资金环境验证风控与执行逻辑后再上实盘。

---

## 七、快速命令汇总

```bash
# 安装 Rust
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
source "$HOME/.cargo/env"

# 部署与编译
cd /opt/poly2
cargo build --release

# 配置 src/.env 后验证
./target/release/poly2 healthcheck
./target/release/poly2 scan-arb

# 使用 systemd 长期运行
sudo systemctl enable --now poly2
journalctl -u poly2 -f
```

按上述步骤即可在 Ubuntu 24.04.2 LTS 上完成部署与运行。
