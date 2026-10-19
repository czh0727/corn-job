package task

import (
	"context"
	"encoding/json"
	"errors"
)

const (
	// ScheduleCron 表示按 Cron 表达式周期触发的任务。
	ScheduleCron = "CRON"
	// ScheduleDelayed 表示在指定时间执行一次的延时任务。
	ScheduleDelayed = "DELAYED"

	// DefinitionDraft 表示任务定义尚未发布。
	DefinitionDraft = "DRAFT"
	// DefinitionActive 表示任务定义已经启用并参与调度。
	DefinitionActive = "ACTIVE"
	// DefinitionPaused 表示任务定义已暂停后续调度。
	DefinitionPaused = "PAUSED"
	// DefinitionFinished 表示一次性延时任务已经结束。
	DefinitionFinished = "FINISHED"

	// WorkerRunning 表示任务 Worker 应当运行并消费队列。
	WorkerRunning = "RUNNING"
	// WorkerStopped 表示任务 Worker 已停止消费队列。
	WorkerStopped = "STOPPED"

	// ExecutionRunning 表示一次任务执行正在处理中。
	ExecutionRunning = "RUNNING"
	// ExecutionRetrying 表示一次任务执行失败后等待重试。
	ExecutionRetrying = "RETRYING"
	// ExecutionSucceeded 表示一次任务执行成功完成。
	ExecutionSucceeded = "SUCCEEDED"
	// ExecutionFailed 表示一次任务执行最终失败。
	ExecutionFailed = "FAILED"
	// ExecutionCanceled 表示一次任务执行已被取消。
	ExecutionCanceled = "CANCELED"

	// MaxPayloadBytes 是单个任务业务参数允许的最大字节数。
	MaxPayloadBytes = 64 * 1024
	// MaxRetryCount 是单个任务允许配置的最大失败重试次数。
	MaxRetryCount = 20
	// MaxConcurrency 是单个 Worker 允许配置的最大并发数。
	MaxConcurrency = 10
	// MaxTotalConcurrency 是当前任务运行时允许的 Worker 总并发数。
	MaxTotalConcurrency = 100
)

var (
	ErrNotFound      = errors.New("task not found")
	ErrConflict      = errors.New("task state conflict")
	ErrInvalidConfig = errors.New("invalid task config")
)

// permanentError 包装不应再由 Asynq 重试的业务错误。
type permanentError struct {
	// err 保存原始业务错误。
	err error
}

// Error 返回不可重试业务错误的原始信息。
func (e permanentError) Error() string { return e.err.Error() }

// Unwrap 暴露原始错误，便于 errors.Is 和 errors.As 判断。
func (e permanentError) Unwrap() error { return e.err }

// Permanent 标记一个业务错误不应再由 Asynq 重试。
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return permanentError{err: err}
}

// IsPermanent 判断错误是否已被业务处理器标记为不可重试。
func IsPermanent(err error) bool {
	var target permanentError
	return errors.As(err, &target)
}

// Definition 描述一个可由 Cron 或指定时间触发的通用任务。
type Definition struct {
	// ID 是任务定义主键。
	ID uint64 `json:"id"`
	// Name 是任务展示名称。
	Name string `json:"name"`
	// Description 是任务用途说明。
	Description string `json:"description"`
	// ScheduleType 指定任务采用 Cron 还是延时调度。
	ScheduleType string `json:"schedule_type"`
	// CronExpression 保存 Cron 任务的调度表达式。
	CronExpression *string `json:"cron_expression,omitempty"`
	// Timezone 指定 Cron 表达式使用的时区。
	Timezone *string `json:"timezone,omitempty"`
	// ExecuteAtMS 是延时任务计划执行的 Unix 毫秒时间戳。
	ExecuteAtMS *int64 `json:"execute_at_ms,omitempty"`
	// Payload 是传递给业务处理器的原始 JSON 参数。
	Payload json.RawMessage `json:"payload"`
	// Status 是当前任务定义状态。
	Status string `json:"status"`
	// CreatedAtMS 是任务定义创建时的 Unix 毫秒时间戳。
	CreatedAtMS int64 `json:"created_at_ms"`
	// UpdatedAtMS 是任务定义最后更新时的 Unix 毫秒时间戳。
	UpdatedAtMS int64 `json:"updated_at_ms"`
	// Worker 是该任务唯一对应的消费配置。
	Worker Worker `json:"worker"`
}

// Worker 保存一个任务对应的业务处理器和消费并发配置。
type Worker struct {
	// ID 是 Worker 配置主键。
	ID uint64 `json:"id"`
	// TaskDefinitionID 是关联的任务定义主键。
	TaskDefinitionID uint64 `json:"task_definition_id"`
	// HandlerType 是 Runtime 注册表中的业务处理器类型。
	HandlerType string `json:"handler_type"`
	// Concurrency 是该 Worker 同时处理任务的最大数量。
	Concurrency int `json:"concurrency"`
	// MaxRetry 是业务执行失败后的最大重试次数。
	MaxRetry int `json:"max_retry"`
	// Status 表示 Worker 是否应当运行。
	Status string `json:"status"`
	// CreatedAtMS 是 Worker 配置创建时的 Unix 毫秒时间戳。
	CreatedAtMS int64 `json:"created_at_ms"`
	// UpdatedAtMS 是 Worker 配置最后更新时的 Unix 毫秒时间戳。
	UpdatedAtMS int64 `json:"updated_at_ms"`
}

// Execution 是由通用执行中间件维护的单次任务执行记录。
type Execution struct {
	// ID 是与 Asynq Task ID 一致的执行标识。
	ID string `json:"id"`
	// TaskDefinitionID 是本次执行所属的任务定义主键。
	TaskDefinitionID uint64 `json:"task_definition_id"`
	// Status 是本次执行的当前结果状态。
	Status string `json:"status"`
	// ErrorMessage 保存最近一次或最终失败原因。
	ErrorMessage *string `json:"error_message,omitempty"`
	// StartedAtMS 是首次开始执行的 Unix 毫秒时间戳。
	StartedAtMS int64 `json:"started_at_ms"`
	// FinishedAtMS 是执行产生最终结果时的 Unix 毫秒时间戳。
	FinishedAtMS *int64 `json:"finished_at_ms,omitempty"`
}

// CreateInput 汇总创建任务定义及其唯一 Worker 所需的字段。
type CreateInput struct {
	// Name 是任务展示名称。
	Name string
	// Description 是任务用途说明。
	Description string
	// ScheduleType 指定任务采用 Cron 还是延时调度。
	ScheduleType string
	// CronExpression 保存 Cron 任务的调度表达式。
	CronExpression *string
	// Timezone 指定 Cron 表达式使用的时区。
	Timezone *string
	// ExecuteAtMS 是延时任务计划执行的 Unix 毫秒时间戳。
	ExecuteAtMS *int64
	// Payload 是传递给业务处理器的原始 JSON 参数。
	Payload json.RawMessage
	// HandlerType 指定处理该任务的已注册业务处理器。
	HandlerType string
	// Concurrency 指定 Worker 的消费并发数。
	Concurrency int
	// MaxRetry 指定业务执行失败后的最大重试次数。
	MaxRetry int
	// WorkerStatus 指定 Worker 创建后的初始状态。
	WorkerStatus string
}

// UpdateInput 保存任务定义 PATCH 后需要重新校验的可选字段。
type UpdateInput struct {
	// Name 在非空时更新任务展示名称。
	Name *string
	// Description 在非空时更新任务用途说明。
	Description *string
	// ScheduleType 在非空时更新任务调度类型。
	ScheduleType *string
	// CronExpression 在非空时更新 Cron 调度表达式。
	CronExpression *string
	// Timezone 在非空时更新 Cron 表达式使用的时区。
	Timezone *string
	// ExecuteAtMS 在非空时更新延时任务执行时间。
	ExecuteAtMS *int64
	// Payload 在非空时更新业务处理器参数。
	Payload *json.RawMessage
}

// WorkerUpdateInput 保存 Worker PATCH 可修改的业务处理器、并发和重试配置。
type WorkerUpdateInput struct {
	// HandlerType 在非空时更新业务处理器类型。
	HandlerType *string
	// Concurrency 在非空时更新 Worker 消费并发数。
	Concurrency *int
	// MaxRetry 在非空时更新最大失败重试次数。
	MaxRetry *int
}

// Job 是通用运行时传递给业务处理器的稳定上下文。
type Job struct {
	// ExecutionID 是与 Asynq Task ID 一致的本次执行标识。
	ExecutionID string
	// TaskDefinitionID 是本次执行所属的任务定义主键。
	TaskDefinitionID uint64
	// Payload 是入队时保存的业务参数快照。
	Payload json.RawMessage
}

// Handler 约束业务任务只实现参数校验和业务执行逻辑。
type Handler interface {
	ValidatePayload(json.RawMessage) error
	Execute(context.Context, Job) error
}
