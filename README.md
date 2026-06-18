# TimeNotesServer

TimeNotesServer 是 TimeNotes 桌面手账应用的多人协作服务端。负责创建协作房间、校验房间密钥、保存 Yjs 文档状态、转发 WebRTC 信令，提供同端口 UDP STUN，并在 P2P DataChannel 不可用时提供应用层服务器中转。

## 技术栈

- **语言**：Go 1.26
- **HTTP 框架**：[Fiber v3](https://github.com/gofiber/fiber)
- **WebSocket**：[gofiber/contrib/websocket](https://github.com/gofiber/contrib)
- **数据库**：SQLite（[modernc.org/sqlite](https://modernc.org/sqlite)，纯 Go 实现，无 CGO 依赖）
- **存储接口**：`internal/storage.Store` 抽象隔离，后续迁移 PostgreSQL 时新增实现即可，不改协议层
- **速率限制**：`golang.org/x/time/rate` 令牌桶

## 协作流程

1. 房主在客户端点击"发起联机"。
2. 客户端调用 `POST /api/rooms`，服务端生成 `roomId` 和 256-bit 高熵 `roomKey`。
3. 服务端只保存 `HMAC-SHA256(roomId, 0x00, roomKey)` 的哈希值，明文 `roomKey` 不写数据库、不写日志。
4. 服务端返回 `wsUrl`、`iceServers` 和邀请链接——邀请链接把 `roomKey` 放在 URL fragment（`#` 之后），不走 HTTP Referer 也避免被代理日志捕获。
5. 协作者通过邀请链接打开客户端，连接 `GET /ws/collab`。**浏览器 WebSocket 握手必须通过 Origin 校验**（复用 CORS 白名单逻辑）。
6. WebSocket 首帧必须是 `auth`，包含 `roomId + roomKey`。校验通过后：
   - 第一位加入者立即成为**房主**（host），拿到在线 peers 和完整的 Yjs 状态。
   - 后续加入者进入 `join_pending` 状态，不能收发文档、聊天或信令。
   - 服务端向房主发送 `join_request`，房主可通过 `join_decision` 批准或拒绝。拒绝/超时（60s）返回 `join_rejected`。
7. 批准后返回 `auth_ok`，包含在线成员列表和 SQLite 中保存的 Yjs compact state + 增量 updates。
8. **房主迁移**：房主非正常断开时，服务端自动将角色迁移给加入最早的协作者，广播 `host_changed`。仅最后一个成员退出时房间才会真正关闭。
9. **踢人**：房主可通过 `peer_kick` 踢出指定协作者，被踢者收到 `peer_kicked` 后退出，其他人收到 `peer_left`。
10. 客户端之间优先建立 WebRTC DataChannel：
    - `doc`：可靠有序，传输 Yjs update。
    - `presence`：低延迟，传输鼠标、当前页面、选中元素、正在编辑元素。
    - `chat`：聊天消息。
11. P2P 失败时同一 envelope 通过服务器 relay 广播或定向投递。该 relay 是 TimeNotes 应用层中转，不是浏览器 ICE 可使用的标准 TURN。
12. **自动压缩**：当某房间的 `room_updates` 增量超过 200 条时，服务端向房主发送 `compaction_request`，提示客户端生成新 Yjs snapshot 上传以清空历史增量。

## 快速启动（本地开发）

```powershell
cd D:\TimeNotes\TimeNotesServer
Copy-Item .\config.example.json .\config.json
# 编辑 config.json，至少调整 secret 为强随机值
go run .
```

默认监听 `127.0.0.1:8787`。

健康检查：
```powershell
Invoke-RestMethod http://127.0.0.1:8787/healthz
```

状态监控：
```powershell
Invoke-RestMethod http://127.0.0.1:8787/stats
# → {"rooms":2,"clients":5,"uptime":"3h15m"}
```

## 完整 JSON 配置说明

服务端默认读取当前目录下的 `config.json`。可用 `TIMENOTES_CONFIG` 环境变量指向其他文件：

```powershell
$env:TIMENOTES_CONFIG = "D:\TimeNotes\TimeNotesServer\config.production.json"
go run .
```

### 网络与存储

| 字段 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `addr` | string | `"127.0.0.1:8787"` | HTTP/WS TCP + STUN UDP 监听地址。跨设备部署设为 `"0.0.0.0:8787"`。 |
| `dbPath` | string | `"data/timenotes-collab.db"` | SQLite 数据库路径。生产建议指向持久卷目录。 |
| `logPath` | string | `"logs/timenotes-collab.log"` | 日志文件路径。 |
| `logMaxBytes` | int64 | `5242880` | 启动时日志超过该大小会被截断。 |

### 安全

| 字段 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `secret` | string | `""` → `"timenotes-dev-secret"` | roomKey HMAC 密钥。**公网部署必须配置强随机值**。监听非 loopback 地址时默认为空或开发默认值会拒绝启动。 |
| `corsOrigins` | []string | `[]` | 允许调用 API 的 Origin 白名单。也控制 WebSocket Origin 校验。 |
| `allowLoopbackOrigins` | bool | `true` | 是否放行 `127.0.0.1` / `localhost` / `wails.localhost`。生产环境建议 `false`。 |
| `insecureAllowDefaultSecret` | bool | `false` | 仅本地开发：允许非 loopback 监听时仍使用默认 secret。**生产绝对不能开启**。 |
| `allowedServerHosts` | []string | `[]` | `POST /api/rooms` 接口中客户端上报的 `serverUrl` 白名单。空表示不限制 host。 |

### 消息与存储大小上限（宽松档默认值）

| 字段 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `maxMessageBytes` | int64 | `134217728` (128 MB) | 单条 WebSocket 消息上限。 |
| `maxUpdateBytes` | int64 | `8388608` (8 MB) | 单条 Yjs 增量 update 二进制上限。超限返回 `doc_update_too_large`。 |
| `maxSnapshotBytes` | int64 | `67108864` (64 MB) | 单次 Yjs 全量 snapshot 二进制上限。超限返回 `doc_snapshot_too_large`。 |
| `roomMaxStorageBytes` | int64 | `536870912` (512 MB) | 单房间累计存储上限。超限返回 `room_storage_exceeded`。 |

### 超时与清理

| 字段 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `authTimeout` | duration | `"15s"` | WebSocket 首帧 auth 等待超时。慢网络可调大。 |
| `readDeadline` | duration | `"30s"` | 认证后单次消息读超时，支撑心跳检测。 |
| `roomTTLDays` | int | `30` | 已关闭房间的保留天数，超期后由清理 goroutine 删除。 |
| `cleanupInterval` | duration | `"1h"` | 清理 goroutine 执行间隔。 |
| `maxRoomsPerIPPerMinute` | int | `10` | 单 IP 每分钟最多创建房间数。 |

### 环境变量覆盖

部署平台可用环境变量覆盖 JSON 配置，无需修改配置文件：

| 变量 | 覆盖字段 |
|------|---------|
| `TIMENOTES_CONFIG` | 配置文件路径 |
| `TIMENOTES_SECRET` | `secret` |
| `TIMENOTES_ADDR` | `addr` |
| `TIMENOTES_DB` | `dbPath` |
| `TIMENOTES_LOG` | `logPath` |
| `TIMENOTES_CORS_ORIGINS` | `corsOrigins`（逗号分隔） |
| `TIMENOTES_LOG_MAX_BYTES` | `logMaxBytes` |
| `TIMENOTES_MAX_MESSAGE_BYTES` | `maxMessageBytes` |
| `TIMENOTES_MAX_UPDATE_BYTES` | `maxUpdateBytes` |
| `TIMENOTES_MAX_SNAPSHOT_BYTES` | `maxSnapshotBytes` |
| `TIMENOTES_ROOM_MAX_STORAGE` | `roomMaxStorageBytes` |
| `TIMENOTES_AUTH_TIMEOUT` | `authTimeout`（Go duration 格式） |
| `TIMENOTES_ROOM_TTL_DAYS` | `roomTTLDays` |
| `TIMENOTES_INSECURE_DEFAULT_SECRET` | `insecureAllowDefaultSecret` |

## 日志

服务端日志同时写入 stdout 和 `TIMENOTES_LOG` 指向的文件。日志只含排障元数据（`room`、`client`、消息类型、字节数、投递数、错误原因），绝不记录：

- 房间密钥明文
- 聊天正文
- Yjs update 或 snapshot 原始内容
- WebRTC SDP 原文

## 数据库

SQLite，使用 WAL 模式 + 立即事务锁 (`_txlock=immediate`) + 外键约束启用。

表结构：

- **`rooms`**：`room_id`、HMAC `key_hash`、`compact_state` (BLOB)、`update_seq`、`created_at`、`updated_at`、`closed_at`。`PRIMARY KEY (room_id)`。
- **`room_updates`**：`room_id`、`seq`、`update_blob` (BLOB)、`created_at`。`PRIMARY KEY (room_id, seq)`，外键 CASCADE。

presence / chat / 鼠标 / 正在编辑元素不落库，仅在线转发。

**后台清理**：每小时（`cleanupInterval`）扫描一次，先删 `room_updates` 再删 `rooms`，清理 `closed_at` 非空且 `updated_at` 超过 `roomTTLDays` 天的房间。

## 新协议消息类型（v1 补充）

本轮修复新增（向后兼容，客户端忽略未知类型即可）：

| 类型常量 | 用途 | 方向 |
|---------|------|------|
| `doc_update_rejected` | 服务端写库失败 → 来源客户端触发 Yjs 重同步 | server → client |
| `compaction_request` | 增量超 200 条 → 房主应生成新 snapshot | server → host |
| `host_changed` | 房主迁移后通知所有协作者更新 local hostId | server → all |
| `sync_request` | 写库失败后全局广播，触发全房间重拉 SQLite 状态 | server → all |

## PostgreSQL 迁移边界

第一阶段不要将 PostgreSQL 逻辑写入协议层。迁移时：

1. 新增 `internal/storage/postgres`。
2. 实现 `internal/storage.Store` 接口。
3. 新增 PostgreSQL 迁移 SQL。
4. 在 `main.go` 用环境变量选择 SQLite 或 PostgreSQL 实现。
5. 不修改 WebSocket envelope，不修改前端协作协议。

## 生产部署清单

按以下顺序逐项执行，不要遗漏。

### 1. 创建生产配置文件

```json
{
  "addr": "127.0.0.1:8787",
  "dbPath": "/var/lib/timenotes/collab.db",
  "logPath": "/var/log/timenotes/collab.log",
  "logMaxBytes": 104857600,
  "secret": "use-strong-random-at-least-32-bytes",
  "corsOrigins": ["https://notes.example.com"],
  "allowLoopbackOrigins": false,
  "maxMessageBytes": 134217728,
  "maxUpdateBytes": 8388608,
  "maxSnapshotBytes": 67108864,
  "roomMaxStorageBytes": 536870912,
  "authTimeout": "15s",
  "readDeadline": "30s",
  "roomTTLDays": 30,
  "cleanupInterval": "1h",
  "maxRoomsPerIPPerMinute": 10,
  "allowedServerHosts": ["example.com"],
  "insecureAllowDefaultSecret": false
}
```

### 2. 生成强随机 secret

```powershell
# 方法 A：OpenSSL
openssl rand -hex 32

# 方法 B：PowerShell
-join ((48..57)+(65..90)+(97..122) | Get-Random -Count 64 | ForEach-Object {[char]$_})
```

### 3. HTTPS / WSS 反代（可选）

推荐 Nginx / Caddy / 云负载均衡终止 TLS，反代到 `127.0.0.1:8787`。

**Nginx 最少配置**：

```nginx
server {
    listen 443 ssl http2;
    server_name notes.example.com;
    ssl_certificate     /etc/ssl/example.com/fullchain.pem;
    ssl_certificate_key /etc/ssl/example.com/privkey.pem;

    location / {
        proxy_pass http://127.0.0.1:8787;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
        proxy_set_header Host $host;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_read_timeout 3600s;
        proxy_send_timeout 3600s;
    }
}
```

> 重要：`proxy_read_timeout` 必须大于 `readDeadline`（默认 30s）的 2 倍以上（建议 3600s），否则正常的 WebSocket 空闲连接会被 Nginx 提前断开。

### 4. 防火墙 / 安全组

- 只开放 `443/tcp`（HTTPS + WSS）。
- UDP STUN 端口：如果用户可能在严格 NAT 后使用 P2P，需额外放行 `8787/udp`。
- 不要直接暴露 `8787/tcp` 到公网。

### 5. 密钥管理

- `secret` 是长期机密，绝对不要提交到 Git。
- 推荐用 `TIMENOTES_SECRET` 环境变量覆盖 JSON 中的占位值，配合 systemd `EnvironmentFile` 或 Docker secrets。
- 更换 secret 会使所有旧 roomKey 邀请链接失效。

### 6. 持久化与备份（可选）

- `/var/lib/timenotes/collab.db` 必须位于持久磁盘。
- 定期备份：`sqlite3 /var/lib/timenotes/collab.db ".backup /backup/collab-$(date +%Y%m%d).db"`
- SQLite 只适合单实例部署。不要让多个进程同时写同一个 `.db` 文件。

### 7. 日志轮转（可选）

服务端自身只做启动时体积保护（`logPath` 超 `logMaxBytes` 就截断）。生产环境建议：

```conf
# /etc/logrotate.d/timenotes
/var/log/timenotes/collab.log {
    daily
    rotate 30
    compress
    delaycompress
    missingok
    notifempty
    copytruncate
}
```

或用 `journald` / Docker logging driver / 云日志服务替代文件日志。

### 8. systemd 守护（用nohup替代也行）

```ini
# /etc/systemd/system/timenotes-collab.service
[Unit]
Description=TimeNotes Collaboration Server
After=network-online.target

[Service]
WorkingDirectory=/opt/timenotes-server
ExecStart=/opt/timenotes-server/timenotesserver
Environment=TIMENOTES_CONFIG=/etc/timenotes/server.json
EnvironmentFile=-/etc/timenotes/secrets.env
Restart=always
RestartSec=3
User=timenotes
Group=timenotes
LimitNOFILE=65536

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now timenotes-collab
sudo systemctl status timenotes-collab
```

### 9. Docker 部署（可选）

```dockerfile
FROM golang:1.26 AS builder
WORKDIR /build
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o timenotesserver .

FROM gcr.io/distroless/static-debian12
WORKDIR /app
COPY --from=builder /build/timenotesserver .
EXPOSE 8787
CMD ["./timenotesserver"]
```

```yaml
# compose.yml
services:
  collab:
    build: .
    ports:
      - "127.0.0.1:8787:8787"
      - "8787:8787/udp"
    environment:
      TIMENOTES_CONFIG: /etc/timenotes/server.json
      TIMENOTES_SECRET: ${TIMENOTES_SECRET}
    volumes:
      - ./config.json:/etc/timenotes/server.json:ro
      - collab-data:/app/data
      - collab-logs:/app/logs
volumes:
  collab-data:
  collab-logs:
```

### 10. 监控

| 端点 | 返回示例 |
|------|---------|
| `GET /healthz` | `{"ok":true,"service":"timenotes-collab"}` |
| `GET /stats` | `{"rooms":3,"clients":7,"uptime":"12h30m"}` |

建议配置：
- 进程存活：`systemctl` 或 Docker healthcheck 调 `/healthz`
- 系统资源：CPU / 内存 / 磁盘 / SQLite 文件大小
- 日志告警：grep 日志中的 `failed` / `err=` / `panic`
- 连接趋势：轮询 `/stats`

### 11. 容量保护总结

| 防护层 | 机制 |
|--------|------|
| WebSocket 单帧 | `maxMessageBytes`（128 MB 默认） |
| Yjs 单条增量 | `maxUpdateBytes`（8 MB 默认） |
| Yjs 单次快照 | `maxSnapshotBytes`（64 MB 默认） |
| 单房间总存储 | `roomMaxStorageBytes`（512 MB 默认） |
| 消息速率 | 每客户端 `rate.Limiter` 200 msg/s，连续 3 次超限断开 |
| 创建房间频率 | 每 IP `rate.Limiter` 10/min |
| WebSocket Origin | 复用 CORS 白名单 + loopback 策略 |
| 房间 TTL | 后台每小时清理关闭超 30 天的房间 |
| 私密数据 | roomKey 只存 HMAC 哈希，不写日志；chat/SDP 不入库 |

**公网部署额外建议**：在反向代理层加 `limit_req` / `limit_conn`（Nginx）做二次防护；单 IP 最大并发 WebSocket 连接数建议限制在 10 以内。

## 端口总览

| 端口 | 协议 | 用途 |
|------|------|------|
| `8787` | TCP | HTTP API + WebSocket |
| `8787` | UDP | 内置 STUN（与 TCP 同端口复用） |

## 常用命令

```powershell
# 测试
go test ./...

# 构建
go build .

# 前端联调
# 前端 dev: http://127.0.0.1:9245
# 服务端:   ws://127.0.0.1:8787/ws/collab
```

## 安全规则（开发者）

- **roomKey**：只保存 HMAC-SHA256 哈希。明文不写数据库、不写日志、不在 HTTP 响应 body 中出现在 `#` 之前。
- **secret**：生产必须配置。修改 secret 让所有旧邀请链接失效是预期行为。
- **日志**：不记录 roomKey 明文、聊天正文、Yjs 原始数据、WebRTC SDP。
- **CORS + WebSocket Origin**：使用同一套白名单逻辑。生产必须精确配置 `corsOrigins` + `allowedServerHosts`。
- **消息大小**：服务端对所有 doc_update / doc_snapshot 做二进制大小校验，超限返回结构化错误。
- **权限**：只有房主能审批加入请求、踢人。消息 `from` 字段由服务端覆盖，不信任客户端自报。
