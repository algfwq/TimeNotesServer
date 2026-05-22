# TimeNotesServer

TimeNotesServer 是 TimeNotes 的多人协作服务端。它负责创建协作房间、校验房间密钥、保存 Yjs 文档状态、转发 WebRTC 信令，提供同端口 UDP STUN，并在 P2P DataChannel 不可用时提供应用层服务器中转。

## 技术栈

- Go 1.26
- Fiber v3
- `github.com/gofiber/contrib/v3/websocket`
- SQLite 第一阶段持久化
- `storage.Store` 存储接口隔离，后续迁移 PostgreSQL 时新增实现即可

## 协作流程

1. 房主在客户端点击发起联机。
2. 客户端调用 `POST /api/rooms`，服务端生成 `roomId` 和高熵 `roomKey`。
3. 服务端只保存 `roomId + roomKey` 的 HMAC 哈希，明文 `roomKey` 不写数据库、不写日志。
4. 服务端返回 `wsUrl`、`iceServers` 和邀请链接，邀请链接把 `roomKey` 放在 URL fragment。
5. 协作者通过邀请链接加入，客户端连接 `GET /ws/collab`。
6. WebSocket 首帧必须是 `auth`；`roomId + roomKey` 校验通过后，后续加入者先进入 `join_pending`，不能收发文档、聊天、presence 或信令。
7. 服务端向房主发送 `join_request`，房主通过客户端弹窗发送 `join_decision`。批准后服务端才返回 `auth_ok`，其中包含在线成员和 SQLite 中保存的 Yjs 状态；拒绝、超时或房主离线时返回 `join_rejected`。
8. 第一位加入房间的用户成为房主，后续获准加入者是协作者。房主可以通过 `peer_kick` 踢出指定协作者，被踢者收到 `peer_kicked` 后退出，其他成员收到 `peer_left`。
9. 客户端之间优先建立 WebRTC DataChannel：
   - `doc`：可靠有序，传输 Yjs update。
   - `presence`：低延迟，传输鼠标、当前页面、选中元素、正在编辑元素。
   - `chat`：聊天消息。
10. P2P 失败、断开或强制中转时，同一 envelope 通过服务器 relay 广播或定向投递。该 relay 是 TimeNotes 应用层中转，不是浏览器 ICE 可使用的标准 TURN。
11. 房主退出时服务端广播 `room_closed`，其他协作者自动退出，房间被持久标记为关闭，旧邀请链接失效。

## 本地启动

```powershell
cd D:\TimeNotes\TimeNotesServer
Copy-Item .\config.example.json .\config.json
# 编辑 config.json，至少修改 secret
go run .
```

默认监听：

```text
127.0.0.1:8787
```

健康检查：

```powershell
Invoke-RestMethod http://127.0.0.1:8787/healthz
```

## JSON 配置

服务端默认读取当前目录下的 `config.json`。可以用 `TIMENOTES_CONFIG` 指向其他配置文件：

```powershell
$env:TIMENOTES_CONFIG = "D:\TimeNotes\TimeNotesServer\config.production.json"
go run .
```

示例见 [config.example.json](./config.example.json)。

| 字段 | 默认值 | 说明 |
| --- | --- | --- |
| `addr` | `127.0.0.1:8787` | 服务监听地址。本机开发用默认值，跨设备或容器内通常设为 `0.0.0.0:8787`。 |
| `dbPath` | `data/timenotes-collab.db` | SQLite 数据库路径。 |
| `logPath` | `logs/timenotes-collab.log` | 日志文件路径。 |
| `logMaxBytes` | `5242880` | 启动时日志文件超过该大小会截断。 |
| `secret` | 空 | 房间密钥 HMAC 的服务端机密。生产环境必须配置强随机值。 |
| `corsOrigins` | `[]` | 允许调用 `POST /api/rooms` 的客户端 origin。生产环境必须精确配置。 |
| `allowLoopbackOrigins` | `true` | 是否默认放行本机 loopback origin。生产环境建议设为 `false`。 |
| `maxMessageBytes` | `67108864` | 单条 WebSocket 消息最大字节数。素材重的房间可先保留默认值，生产应结合代理限制调整。 |

仍保留少量环境变量覆盖能力，方便 Docker/systemd 注入敏感参数：`TIMENOTES_CONFIG`、`TIMENOTES_SECRET`、`TIMENOTES_ADDR`、`TIMENOTES_DB`、`TIMENOTES_LOG`、`TIMENOTES_CORS_ORIGINS`、`TIMENOTES_LOG_MAX_BYTES`、`TIMENOTES_MAX_MESSAGE_BYTES`。

## 日志

服务端日志同时写入 stdout 和 `TIMENOTES_LOG` 指向的文件。

日志只记录排障需要的元数据，例如 `room`、`client`、消息类型、字节数、投递数量和错误原因。日志不得记录：

- 房间密钥明文
- 聊天正文
- Yjs update 或 snapshot 原始内容
- WebRTC SDP 原文

## 数据库

当前 SQLite 表：

- `rooms`：房间、密钥哈希、compact Yjs state、最新 update 序号、关闭时间。
- `room_updates`：按 `seq` 追加的 Yjs 二进制增量。

聊天、鼠标位置、在线成员、正在编辑元素等 presence 信息不落库，只在线转发。

## PostgreSQL 迁移边界

第一阶段不要把 PostgreSQL 逻辑写入协议层。迁移时按下面边界推进：

1. 新增 `internal/storage/postgres`。
2. 让该实现满足 `internal/storage.Store`。
3. 新增 PostgreSQL 迁移 SQL。
4. 在 `main.go` 用环境变量选择 SQLite 或 PostgreSQL 实现。
5. 不修改 WebSocket envelope，不修改前端协作协议。

## 生产部署清单

生产环境不能直接用本地开发默认配置。建议按下面顺序执行。

1. 创建生产配置文件，例如 `/etc/timenotes/server.json`：

```json
{
  "addr": "127.0.0.1:8787",
  "dbPath": "/var/lib/timenotes/collab.db",
  "logPath": "/var/log/timenotes/collab.log",
  "logMaxBytes": 104857600,
  "secret": "use-a-strong-random-secret-at-least-32-bytes",
  "corsOrigins": ["https://notes.example.com"],
  "allowLoopbackOrigins": false,
  "maxMessageBytes": 67108864
}
```

2. 用 HTTPS/WSS 对外提供服务。推荐让 Nginx、Caddy、云负载均衡或 API Gateway 终止 TLS，再反代到 `127.0.0.1:8787`。

3. Nginx WebSocket 反代至少需要：

```nginx
location / {
    proxy_pass http://127.0.0.1:8787;
    proxy_http_version 1.1;
    proxy_set_header Upgrade $http_upgrade;
    proxy_set_header Connection "upgrade";
    proxy_set_header Host $host;
    proxy_read_timeout 3600s;
    proxy_send_timeout 3600s;
}
```

4. 在云服务器安全组和系统防火墙中只开放 `443/tcp`。如果不放反代而直接暴露 Go 服务，才开放 `8787/tcp`，但不推荐。

5. 把 `secret` 当成长期机密管理。不要提交到 Git；可以用 `TIMENOTES_SECRET` 覆盖 JSON 文件里的占位值。

6. 给 `/var/lib/timenotes` 配置持久磁盘、快照和备份。SQLite 阶段只适合单实例部署，不要让多个服务进程同时写同一个 SQLite 文件。

7. 给日志配置系统级轮转。服务端自身只做启动时体积保护，生产应使用 `logrotate`、journald、Docker logging driver 或云日志服务。

8. 配置进程守护。systemd 示例：

```ini
[Unit]
Description=TimeNotes Collaboration Server
After=network-online.target

[Service]
WorkingDirectory=/opt/timenotes-server
ExecStart=/opt/timenotes-server/timenotesserver
Environment=TIMENOTES_CONFIG=/etc/timenotes/server.json
Restart=always
RestartSec=3
User=timenotes
Group=timenotes

[Install]
WantedBy=multi-user.target
```

9. 配置监控和告警：进程存活、`/healthz`、CPU、内存、磁盘、SQLite 文件大小、日志错误数量、WebSocket 连接数。

10. 做容量保护：限制单 IP 连接数、房间数、消息大小、创建房间频率。当前代码已有单消息上限，但公网部署还需要反向代理和防火墙层限流。

11. 放行 UDP STUN。服务端会在 `addr` 对应的同一端口监听 UDP STUN，例如 HTTP/WS 为 `:8787` 时 STUN 也是 UDP `:8787`。P2P 在严格 NAT 下仍不保证成功；当前内置中转是应用层 relay，不是标准 TURN。

12. 多实例部署前先迁移 PostgreSQL，并增加跨实例房间路由或消息总线。当前内存房间表要求同一个房间的 WebSocket 连接落在同一个进程。

## 常用验证命令

```powershell
go test ./...
go build .
```

前端联调时，默认使用：

```text
http://127.0.0.1:9245
ws://127.0.0.1:8787/ws/collab
```
