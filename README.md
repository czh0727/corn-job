# corn-job

从 Matrix Admin 独立出的定时任务管理小项目，包含任务控制台和后端接口。保留任务定义、Cron / 延时调度、Worker 启停、手动运行、取消执行和执行记录。任务运行由 Asynq 协调，MySQL 保存定义和执行状态，Redis 保存队列和选主租约。

目前内置 `log` 处理器：接受任意合法 JSON payload，执行时直接写到服务标准输出。原 Admin 的 `tenantcreditcallback` 涉及其他业务系统，不在本项目中注册。

## 启动

需要 Go 1.26.1 和 Docker。先启动依赖，再启动服务：

```sh
docker compose up -d --wait
go run ./cmd/corn-job
```

打开 [http://127.0.0.1:8080/ui/](http://127.0.0.1:8080/ui/) 使用任务控制台。前端页面由 Go 服务直接提供，无需单独启动开发服务器。

首次创建 MySQL 数据卷时会执行 `migrations/001_init.sql`。若沿用已有数据卷，请自行应用该 SQL。默认 API 监听 `127.0.0.1:8080`，MySQL 映射到 `127.0.0.1:3310`，Redis 映射到 `127.0.0.1:6380`。可用 `LISTEN_ADDR`、`MYSQL_DSN`、`REDIS_ADDR` 覆盖连接地址；默认配置仅供本地开发。

```sh
go test ./...
node --test web/app.test.mjs
```

## 接口

接口路径延续 Admin 的 `/api/v1/task-definitions` 和 `/api/v1/task-executions`。服务只绑定本机地址，没有包含 Admin 的登录与权限系统；如需对外提供，请在接入层配置鉴权。

示例：创建一个每分钟执行的任务，然后发布并启动 Worker。

```sh
curl -X POST http://127.0.0.1:8080/api/v1/task-definitions \
  -H 'Content-Type: application/json' \
  -d '{"name":"demo","schedule_type":"CRON","cron_expression":"* * * * *","timezone":"Asia/Shanghai","payload":{"message":"hello"},"worker":{"handler_type":"log","concurrency":1,"max_retry":3,"status":"RUNNING"}}'

curl -X POST http://127.0.0.1:8080/api/v1/task-definitions/1/publish
curl -X POST http://127.0.0.1:8080/api/v1/task-definitions/1/run
curl http://127.0.0.1:8080/api/v1/task-executions?task_definition_id=1
```

定义接口支持创建、列表、详情、修改、发布、暂停、恢复、立即执行和 Worker 查询、修改、启停。执行接口支持分页查询、详情和取消。健康检查为 `GET /healthz`。
