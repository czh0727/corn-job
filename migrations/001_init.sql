CREATE TABLE IF NOT EXISTS admin_task_definitions (
  id              BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '任务定义主键',
  name            VARCHAR(128) NOT NULL COMMENT '任务名称',
  description     VARCHAR(500) NOT NULL DEFAULT '' COMMENT '任务说明',
  schedule_type   VARCHAR(16) NOT NULL COMMENT '调度类型：CRON 或 DELAYED',
  cron_expression VARCHAR(128) NULL COMMENT 'CRON 表达式，支持五段式和 @every',
  timezone        VARCHAR(64) NULL COMMENT 'CRON 时区，例如 Asia/Shanghai',
  execute_at_ms   BIGINT NULL COMMENT '延时任务执行时间，Unix 毫秒时间戳',
  payload         JSON NOT NULL COMMENT '业务处理器接收的任务参数',
  status          VARCHAR(16) NOT NULL DEFAULT 'DRAFT' COMMENT '任务状态：DRAFT、ACTIVE、PAUSED 或 FINISHED',
  created_at_ms   BIGINT NOT NULL COMMENT '创建时间，Unix 毫秒时间戳',
  updated_at_ms   BIGINT NOT NULL COMMENT '更新时间，Unix 毫秒时间戳',
  PRIMARY KEY (id),
  KEY idx_admin_task_definitions_status_type (status, schedule_type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Admin 通用任务定义';

CREATE TABLE IF NOT EXISTS admin_task_workers (
  id                 BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'Worker 配置主键',
  task_definition_id BIGINT UNSIGNED NOT NULL COMMENT '关联 admin_task_definitions.id',
  handler_type       VARCHAR(64) NOT NULL COMMENT '业务处理器注册名称',
  concurrency        INT NOT NULL DEFAULT 1 COMMENT '该任务并发 Worker 数量',
  max_retry          INT NOT NULL DEFAULT 3 COMMENT '失败后的最大重试次数，0 表示不重试',
  status             VARCHAR(16) NOT NULL DEFAULT 'STOPPED' COMMENT 'Worker 状态：RUNNING 或 STOPPED',
  created_at_ms      BIGINT NOT NULL COMMENT '创建时间，Unix 毫秒时间戳',
  updated_at_ms      BIGINT NOT NULL COMMENT '更新时间，Unix 毫秒时间戳',
  PRIMARY KEY (id),
  UNIQUE KEY uk_admin_task_workers_definition (task_definition_id),
  KEY idx_admin_task_workers_status (status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Admin 任务 Worker 配置';

CREATE TABLE IF NOT EXISTS admin_task_executions (
  id                 VARCHAR(64) NOT NULL COMMENT 'Asynq Task ID，同一次重试保持不变',
  task_definition_id BIGINT UNSIGNED NOT NULL COMMENT '关联 admin_task_definitions.id',
  status             VARCHAR(16) NOT NULL COMMENT '执行状态：RUNNING、RETRYING、SUCCEEDED、FAILED 或 CANCELED',
  error_message      VARCHAR(2000) NULL COMMENT '最后一次失败原因',
  started_at_ms      BIGINT NOT NULL COMMENT '首次开始时间，Unix 毫秒时间戳',
  finished_at_ms     BIGINT NULL COMMENT '最终结束时间，Unix 毫秒时间戳',
  PRIMARY KEY (id),
  KEY idx_admin_task_executions_definition_time (task_definition_id, started_at_ms),
  KEY idx_admin_task_executions_status (status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Admin 任务执行日志';
