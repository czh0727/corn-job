package task

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/hibiken/asynq"
	"github.com/redis/go-redis/v9"
)

const (
	asynqTaskType      = "admin:task:execute"
	asynqDispatchType  = "admin:task:dispatch"
	dispatchQueue      = "admin_task_dispatch"
	workerSyncInterval = 5 * time.Second
)

type taskStore interface {
	CreateTask(context.Context, CreateInput, int64) (*Definition, error)
	ListTasks(context.Context) ([]Definition, error)
	TaskByID(context.Context, uint64) (*Definition, error)
	ListActiveCronTasks(context.Context) ([]Definition, error)
	ListRunningTaskWorkers(context.Context) ([]Worker, error)
	ListFinishedDelayedTaskWorkers(context.Context) ([]Worker, error)
	UpdateTaskDefinition(context.Context, Definition, int64) error
	UpdateTaskWorker(context.Context, Worker, int64) error
	UpdateTaskStatus(context.Context, uint64, string, int64) error
	UpdateTaskWorkerStatus(context.Context, uint64, string, int64) error
	StartTaskExecution(context.Context, string, uint64, int64) error
	FinishTaskExecution(context.Context, string, string, *string, *int64) error
	ListExecutions(context.Context, *uint64, string, int, int) ([]Execution, int64, error)
	ExecutionByID(context.Context, string) (*Execution, error)
}

// Runtime 统一管理任务定义、Asynq Scheduler、动态 Worker 和执行日志中间件。
type Runtime struct {
	store          taskStore
	redis          redis.UniversalClient
	client         *asynq.Client
	inspector      *asynq.Inspector
	schedulerRedis asynq.RedisConnOpt
	log            *slog.Logger

	registryMu sync.RWMutex
	handlers   map[string]Handler

	lifecycleMu sync.Mutex
	cancel      context.CancelFunc
	wg          sync.WaitGroup
	reconcileCh chan struct{}
}

// NewRuntime 创建通用任务运行时；Scheduler 使用可独立关闭的同配置 Redis 连接。
func NewRuntime(store taskStore, redisClient redis.UniversalClient) *Runtime {
	var schedulerRedis asynq.RedisConnOpt
	if client, ok := redisClient.(*redis.Client); ok {
		options := client.Options()
		schedulerRedis = asynq.RedisClientOpt{
			Network:      options.Network,
			Addr:         options.Addr,
			Username:     options.Username,
			Password:     options.Password,
			DB:           options.DB,
			DialTimeout:  options.DialTimeout,
			ReadTimeout:  options.ReadTimeout,
			WriteTimeout: options.WriteTimeout,
			PoolSize:     options.PoolSize,
			TLSConfig:    options.TLSConfig,
		}
	}
	return &Runtime{
		store:          store,
		redis:          redisClient,
		client:         asynq.NewClientFromRedisClient(redisClient),
		inspector:      asynq.NewInspectorFromRedisClient(redisClient),
		schedulerRedis: schedulerRedis,
		log:            slog.New(slog.NewTextHandler(os.Stdout, nil)),
		handlers:       make(map[string]Handler),
		reconcileCh:    make(chan struct{}, 1),
	}
}

// Register 注册一个稳定名称对应的业务任务处理器。
func (r *Runtime) Register(handlerType string, handler Handler) error {
	handlerType = strings.TrimSpace(handlerType)
	if handlerType == "" || handler == nil {
		return fmt.Errorf("%w: handler_type and handler are required", ErrInvalidConfig)
	}
	r.registryMu.Lock()
	defer r.registryMu.Unlock()
	if _, exists := r.handlers[handlerType]; exists {
		return fmt.Errorf("%w: handler %q already registered", ErrConflict, handlerType)
	}
	r.handlers[handlerType] = handler
	return nil
}

// Start 启动运行时选主循环；重复调用不会启动第二套运行时。
func (r *Runtime) Start() error {
	if r == nil || r.store == nil || r.redis == nil || r.schedulerRedis == nil {
		return fmt.Errorf("%w: task runtime dependencies are incomplete", ErrInvalidConfig)
	}
	r.lifecycleMu.Lock()
	defer r.lifecycleMu.Unlock()
	if r.cancel != nil {
		return nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	r.cancel = cancel
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		r.leadershipLoop(ctx)
	}()
	return nil
}

// Shutdown 停止 Scheduler 和所有本实例 Worker，并等待在途任务优雅退出。
func (r *Runtime) Shutdown() {
	if r == nil {
		return
	}
	r.lifecycleMu.Lock()
	cancel := r.cancel
	r.cancel = nil
	r.lifecycleMu.Unlock()
	if cancel == nil {
		return
	}
	cancel()
	r.wg.Wait()
}

// Create 校验调度与业务 payload 后创建任务及 Worker 配置。
func (r *Runtime) Create(ctx context.Context, input CreateInput) (*Definition, error) {
	normalizeCreateInput(&input)
	if err := r.validateCreateInput(input); err != nil {
		return nil, err
	}
	if input.WorkerStatus == WorkerRunning {
		if err := r.validateTotalConcurrency(ctx, 0, input.Concurrency); err != nil {
			return nil, err
		}
	}
	return r.store.CreateTask(ctx, input, time.Now().UnixMilli())
}

// List 返回全部任务定义和 Worker 配置。
func (r *Runtime) List(ctx context.Context) ([]Definition, error) {
	return r.store.ListTasks(ctx)
}

// Get 按主键返回任务定义和 Worker 配置。
func (r *Runtime) Get(ctx context.Context, id uint64) (*Definition, error) {
	return r.store.TaskByID(ctx, id)
}

// Update 合并 PATCH 字段，并只允许修改尚未发布或已经暂停的任务定义。
func (r *Runtime) Update(ctx context.Context, id uint64, input UpdateInput) (*Definition, error) {
	definition, err := r.store.TaskByID(ctx, id)
	if err != nil {
		return nil, err
	}
	if definition.Status == DefinitionActive || definition.Status == DefinitionFinished {
		return nil, fmt.Errorf("%w: pause an active task before editing; finished task is immutable", ErrConflict)
	}
	applyDefinitionUpdate(definition, input)
	validation := CreateInput{
		Name:           definition.Name,
		Description:    definition.Description,
		ScheduleType:   definition.ScheduleType,
		CronExpression: definition.CronExpression,
		Timezone:       definition.Timezone,
		ExecuteAtMS:    definition.ExecuteAtMS,
		Payload:        definition.Payload,
		HandlerType:    definition.Worker.HandlerType,
		Concurrency:    definition.Worker.Concurrency,
		MaxRetry:       definition.Worker.MaxRetry,
		WorkerStatus:   definition.Worker.Status,
	}
	normalizeCreateInput(&validation)
	if err := r.validateCreateInput(validation); err != nil {
		return nil, err
	}
	definition.Name = validation.Name
	definition.Description = validation.Description
	definition.ScheduleType = validation.ScheduleType
	definition.CronExpression = validation.CronExpression
	definition.Timezone = validation.Timezone
	definition.ExecuteAtMS = validation.ExecuteAtMS
	definition.Payload = validation.Payload
	nowMS := time.Now().UnixMilli()
	if err := r.store.UpdateTaskDefinition(ctx, *definition, nowMS); err != nil {
		return nil, err
	}
	definition.UpdatedAtMS = nowMS
	return definition, nil
}

// StartTask 启用任务；延时任务会立即以 ProcessAt 方式写入 Asynq。
func (r *Runtime) StartTask(ctx context.Context, id uint64) error {
	definition, err := r.store.TaskByID(ctx, id)
	if err != nil {
		return err
	}
	if definition.Status == DefinitionActive {
		return nil
	}
	if definition.Status == DefinitionFinished {
		return fmt.Errorf("%w: finished delayed task cannot be started again", ErrConflict)
	}
	if definition.ScheduleType == ScheduleDelayed && (definition.ExecuteAtMS == nil || *definition.ExecuteAtMS <= time.Now().UnixMilli()) {
		return fmt.Errorf("%w: execute_at_ms must be in the future when task starts", ErrInvalidConfig)
	}
	handler, ok := r.handler(definition.Worker.HandlerType)
	if !ok {
		return fmt.Errorf("%w: handler %q is not registered", ErrInvalidConfig, definition.Worker.HandlerType)
	}
	if err := handler.ValidatePayload(definition.Payload); err != nil {
		return fmt.Errorf("%w: payload: %v", ErrInvalidConfig, err)
	}
	if err := r.store.UpdateTaskStatus(ctx, id, DefinitionActive, time.Now().UnixMilli()); err != nil {
		return err
	}
	if definition.ScheduleType == ScheduleDelayed {
		if err := r.enqueueDelayed(definition); err != nil {
			_ = r.store.UpdateTaskStatus(ctx, id, definition.Status, time.Now().UnixMilli())
			return err
		}
	}
	return nil
}

// RunTask 根据当前任务定义立即创建一条新的业务 Job，并返回新的 Execution ID。
func (r *Runtime) RunTask(ctx context.Context, id uint64) (string, error) {
	definition, err := r.store.TaskByID(ctx, id)
	if err != nil {
		return "", err
	}
	if definition.Status == DefinitionDraft {
		return "", fmt.Errorf("%w: publish task before manual run", ErrConflict)
	}
	if definition.Status == DefinitionFinished {
		return "", fmt.Errorf("%w: finished task cannot run again", ErrConflict)
	}
	handler, ok := r.handler(definition.Worker.HandlerType)
	if !ok {
		return "", fmt.Errorf("%w: handler %q is not registered", ErrInvalidConfig, definition.Worker.HandlerType)
	}
	if err := handler.ValidatePayload(definition.Payload); err != nil {
		return "", fmt.Errorf("%w: payload: %v", ErrInvalidConfig, err)
	}
	executionID := "exec-" + uuid.NewString()
	payload, err := marshalEnvelope(*definition, executionID)
	if err != nil {
		return "", err
	}
	_, err = r.client.Enqueue(
		asynq.NewTask(asynqTaskType, payload),
		asynq.Queue(queueName(id)),
		asynq.TaskID(executionID),
		asynq.MaxRetry(definition.Worker.MaxRetry),
	)
	if err != nil {
		return "", fmt.Errorf("enqueue manual task: %w", err)
	}
	return executionID, nil
}

// PauseTask 暂停后续调度；已经进入业务队列的任务仍按入队快照继续执行。
func (r *Runtime) PauseTask(ctx context.Context, id uint64) error {
	definition, err := r.store.TaskByID(ctx, id)
	if err != nil {
		return err
	}
	if definition.Status == DefinitionFinished {
		return fmt.Errorf("%w: finished delayed task is immutable", ErrConflict)
	}
	if definition.Status == DefinitionPaused {
		return nil
	}
	if err := r.store.UpdateTaskStatus(ctx, id, DefinitionPaused, time.Now().UnixMilli()); err != nil {
		return err
	}
	if definition.ScheduleType == ScheduleDelayed {
		// 任务可能已经开始或完成，此时删除失败不影响“暂停后不再执行业务逻辑”的状态兜底。
		_ = r.inspector.DeleteTask(queueName(id), delayedTaskID(id))
	}
	return nil
}

// StartWorker 标记 Worker 为运行状态，由当前 Runtime Leader 创建消费 Server 后再恢复队列。
func (r *Runtime) StartWorker(ctx context.Context, taskID uint64) error {
	definition, err := r.store.TaskByID(ctx, taskID)
	if err != nil {
		return err
	}
	if definition.Worker.Status == WorkerRunning {
		return nil
	}
	if err := r.validateTotalConcurrency(ctx, taskID, definition.Worker.Concurrency); err != nil {
		return err
	}
	if err := r.store.UpdateTaskWorkerStatus(ctx, taskID, WorkerRunning, time.Now().UnixMilli()); err != nil {
		return err
	}
	r.signalReconcile()
	return nil
}

// UpdateWorker 更新 Worker 处理器、并发或失败重试次数，并通知 Leader 收敛运行实例。
func (r *Runtime) UpdateWorker(ctx context.Context, taskID uint64, input WorkerUpdateInput) (*Worker, error) {
	definition, err := r.store.TaskByID(ctx, taskID)
	if err != nil {
		return nil, err
	}
	worker := definition.Worker
	if input.HandlerType != nil {
		handlerType := strings.TrimSpace(*input.HandlerType)
		if handlerType != worker.HandlerType && (definition.Status == DefinitionActive || worker.Status == WorkerRunning) {
			return nil, fmt.Errorf("%w: stop worker and pause task before changing handler_type", ErrConflict)
		}
		worker.HandlerType = handlerType
	}
	if input.Concurrency != nil {
		worker.Concurrency = *input.Concurrency
	}
	if input.MaxRetry != nil {
		worker.MaxRetry = *input.MaxRetry
	}
	if worker.Concurrency < 1 || worker.Concurrency > MaxConcurrency {
		return nil, fmt.Errorf("%w: concurrency must be 1..%d", ErrInvalidConfig, MaxConcurrency)
	}
	if worker.MaxRetry < 0 || worker.MaxRetry > MaxRetryCount {
		return nil, fmt.Errorf("%w: max_retry must be 0..%d", ErrInvalidConfig, MaxRetryCount)
	}
	handler, ok := r.handler(worker.HandlerType)
	if !ok {
		return nil, fmt.Errorf("%w: handler %q is not registered", ErrInvalidConfig, worker.HandlerType)
	}
	if err := handler.ValidatePayload(definition.Payload); err != nil {
		return nil, fmt.Errorf("%w: payload: %v", ErrInvalidConfig, err)
	}
	if worker.Status == WorkerRunning {
		if err := r.validateTotalConcurrency(ctx, taskID, worker.Concurrency); err != nil {
			return nil, err
		}
	}
	nowMS := time.Now().UnixMilli()
	if err := r.store.UpdateTaskWorker(ctx, worker, nowMS); err != nil {
		return nil, err
	}
	worker.UpdatedAtMS = nowMS
	r.signalReconcile()
	return &worker, nil
}

// StopWorker 先暂停 Redis 队列再通知 Runtime Leader 停止对应消费 Server。
func (r *Runtime) StopWorker(ctx context.Context, taskID uint64) error {
	definition, err := r.store.TaskByID(ctx, taskID)
	if err != nil {
		return err
	}
	if definition.Worker.Status == WorkerStopped {
		return nil
	}
	paused, err := r.pauseQueueIfNeeded(queueName(taskID))
	if err != nil {
		return fmt.Errorf("pause task queue: %w", err)
	}
	if err := r.store.UpdateTaskWorkerStatus(ctx, taskID, WorkerStopped, time.Now().UnixMilli()); err != nil {
		if paused {
			_ = r.unpauseQueueIfNeeded(queueName(taskID))
		}
		return err
	}
	r.signalReconcile()
	return nil
}

// 避免定时任务没有发job时队列未创建而启动worker报错
func (r *Runtime) queueInfoIfExists(queue string) (*asynq.QueueInfo, bool, error) {
	queues, err := r.inspector.Queues()
	if err != nil {
		return nil, false, err
	}
	for _, existing := range queues {
		if existing != queue {
			continue
		}
		info, err := r.inspector.GetQueueInfo(queue)
		if err != nil {
			return nil, false, err
		}
		return info, true, nil
	}
	return nil, false, nil
}

// pauseQueueIfNeeded 只在 Asynq 队列存在且尚未暂停时执行暂停。
func (r *Runtime) pauseQueueIfNeeded(queue string) (bool, error) {
	info, exists, err := r.queueInfoIfExists(queue)
	if err != nil {
		return false, err
	}
	if !exists {
		return false, nil
	}
	if info.Paused {
		return false, nil
	}
	if err := r.inspector.PauseQueue(queue); err != nil {
		return false, err
	}
	return true, nil
}

// unpauseQueueIfNeeded 只在 Asynq 队列存在且处于暂停状态时恢复消费。
func (r *Runtime) unpauseQueueIfNeeded(queue string) error {
	info, exists, err := r.queueInfoIfExists(queue)
	if err != nil {
		return err
	}
	if !exists {
		return nil
	}
	if !info.Paused {
		return nil
	}
	return r.inspector.UnpauseQueue(queue)
}

// ListExecutions 分页返回任务执行日志。
func (r *Runtime) ListExecutions(ctx context.Context, taskID *uint64, status string, page, size int) ([]Execution, int64, error) {
	if page < 1 {
		page = 1
	}
	if size < 1 {
		size = 20
	}
	if size > 100 {
		size = 100
	}
	status = strings.ToUpper(strings.TrimSpace(status))
	if status != "" && !validExecutionStatus(status) {
		return nil, 0, fmt.Errorf("%w: invalid execution status", ErrInvalidConfig)
	}
	return r.store.ListExecutions(ctx, taskID, status, page, size)
}

// GetExecution 按 Execution ID 返回单条执行日志。
func (r *Runtime) GetExecution(ctx context.Context, id string) (*Execution, error) {
	return r.store.ExecutionByID(ctx, strings.TrimSpace(id))
}

// CancelExecution 尽力取消 Active Job 或删除排队 Job，并把现有执行日志标记为 CANCELED。
func (r *Runtime) CancelExecution(ctx context.Context, id string) error {
	execution, err := r.store.ExecutionByID(ctx, strings.TrimSpace(id))
	if err != nil {
		return err
	}
	if execution.Status == ExecutionSucceeded || execution.Status == ExecutionFailed || execution.Status == ExecutionCanceled {
		return fmt.Errorf("%w: execution is already finished", ErrConflict)
	}
	if err := r.inspector.CancelProcessing(execution.ID); err != nil {
		return fmt.Errorf("cancel active execution: %w", err)
	}
	if err := r.inspector.DeleteTask(queueName(execution.TaskDefinitionID), execution.ID); err != nil && !errors.Is(err, asynq.ErrTaskNotFound) {
		// Active 任务不能删除，已通过 CancelProcessing 通知其 context 退出。
		r.log.DebugContext(ctx, "queued execution delete skipped", "execution_id", execution.ID, "error", err)
	}
	finishedAt := time.Now().UnixMilli()
	return r.store.FinishTaskExecution(ctx, execution.ID, ExecutionCanceled, nil, &finishedAt)
}

// validateCreateInput 校验数据库之外的调度约束和业务处理器参数。
func (r *Runtime) validateCreateInput(input CreateInput) error {
	if input.Name == "" || len(input.Name) > 128 {
		return fmt.Errorf("%w: name length must be 1..128", ErrInvalidConfig)
	}
	if len(input.Description) > 500 {
		return fmt.Errorf("%w: description is too long", ErrInvalidConfig)
	}
	if len(input.Payload) == 0 || len(input.Payload) > MaxPayloadBytes || !json.Valid(input.Payload) {
		return fmt.Errorf("%w: payload must be valid JSON within %d bytes", ErrInvalidConfig, MaxPayloadBytes)
	}
	if input.Concurrency < 1 || input.Concurrency > MaxConcurrency {
		return fmt.Errorf("%w: concurrency must be 1..%d", ErrInvalidConfig, MaxConcurrency)
	}
	if input.MaxRetry < 0 || input.MaxRetry > MaxRetryCount {
		return fmt.Errorf("%w: max_retry must be 0..%d", ErrInvalidConfig, MaxRetryCount)
	}
	if input.WorkerStatus != WorkerRunning && input.WorkerStatus != WorkerStopped {
		return fmt.Errorf("%w: invalid worker status", ErrInvalidConfig)
	}
	if err := r.validateSchedule(input); err != nil {
		return err
	}
	handler, ok := r.handler(input.HandlerType)
	if !ok {
		return fmt.Errorf("%w: handler %q is not registered", ErrInvalidConfig, input.HandlerType)
	}
	if err := handler.ValidatePayload(input.Payload); err != nil {
		return fmt.Errorf("%w: payload: %v", ErrInvalidConfig, err)
	}
	return nil
}

// validateTotalConcurrency 校验所有运行中 Worker 的配置并发总和。
func (r *Runtime) validateTotalConcurrency(ctx context.Context, excludedTaskID uint64, desiredConcurrency int) error {
	workers, err := r.store.ListRunningTaskWorkers(ctx)
	if err != nil {
		return err
	}
	total := desiredConcurrency
	for _, worker := range workers {
		if worker.TaskDefinitionID != excludedTaskID {
			total += worker.Concurrency
		}
	}
	if total > MaxTotalConcurrency {
		return fmt.Errorf("%w: total running concurrency must not exceed %d", ErrInvalidConfig, MaxTotalConcurrency)
	}
	return nil
}

// applyDefinitionUpdate 把 PATCH 中出现的字段合并到现有任务定义。
func applyDefinitionUpdate(definition *Definition, input UpdateInput) {
	if input.Name != nil {
		definition.Name = *input.Name
	}
	if input.Description != nil {
		definition.Description = *input.Description
	}
	if input.ScheduleType != nil {
		definition.ScheduleType = *input.ScheduleType
		if strings.EqualFold(*input.ScheduleType, ScheduleCron) {
			definition.ExecuteAtMS = nil
		} else if strings.EqualFold(*input.ScheduleType, ScheduleDelayed) {
			definition.CronExpression = nil
			definition.Timezone = nil
		}
	}
	if input.CronExpression != nil {
		definition.CronExpression = input.CronExpression
		if strings.HasPrefix(strings.TrimSpace(*input.CronExpression), "@every ") {
			definition.Timezone = nil
		}
	}
	if input.Timezone != nil {
		definition.Timezone = input.Timezone
	}
	if input.ExecuteAtMS != nil {
		definition.ExecuteAtMS = input.ExecuteAtMS
	}
	if input.Payload != nil {
		definition.Payload = append(json.RawMessage(nil), (*input.Payload)...)
	}
}

// validExecutionStatus 判断执行状态筛选值是否属于通用状态机。
func validExecutionStatus(status string) bool {
	switch status {
	case ExecutionRunning, ExecutionRetrying, ExecutionSucceeded, ExecutionFailed, ExecutionCanceled:
		return true
	default:
		return false
	}
}

// validateSchedule 校验五段式 Cron、秒级 @every 或延时执行时间。
func (r *Runtime) validateSchedule(input CreateInput) error {
	switch input.ScheduleType {
	case ScheduleCron:
		if input.CronExpression == nil || strings.TrimSpace(*input.CronExpression) == "" {
			return fmt.Errorf("%w: cron_expression is required", ErrInvalidConfig)
		}
		expression := strings.TrimSpace(*input.CronExpression)
		isEvery := strings.HasPrefix(expression, "@every ")
		if !isEvery && len(strings.Fields(expression)) != 5 {
			return fmt.Errorf("%w: cron_expression must be five-field cron or @every duration", ErrInvalidConfig)
		}
		if !isEvery && (input.Timezone == nil || strings.TrimSpace(*input.Timezone) == "") {
			return fmt.Errorf("%w: timezone is required for cron task", ErrInvalidConfig)
		}
		if !isEvery {
			if _, err := time.LoadLocation(strings.TrimSpace(*input.Timezone)); err != nil {
				return fmt.Errorf("%w: timezone: %v", ErrInvalidConfig, err)
			}
		} else if input.Timezone != nil {
			return fmt.Errorf("%w: timezone is not used with @every", ErrInvalidConfig)
		}
		if input.ExecuteAtMS != nil {
			return fmt.Errorf("%w: execute_at_ms is only valid for delayed task", ErrInvalidConfig)
		}
		validator := asynq.NewSchedulerFromRedisClient(r.redis, &asynq.SchedulerOpts{Location: time.UTC})
		entryID, err := validator.Register(
			cronSpec(Definition{CronExpression: input.CronExpression, Timezone: input.Timezone}),
			asynq.NewTask(asynqDispatchType, nil),
		)
		if err != nil {
			return fmt.Errorf("%w: cron_expression: %v", ErrInvalidConfig, err)
		}
		validator.Unregister(entryID)
	case ScheduleDelayed:
		if input.ExecuteAtMS == nil || *input.ExecuteAtMS <= time.Now().UnixMilli() {
			return fmt.Errorf("%w: execute_at_ms must be in the future", ErrInvalidConfig)
		}
		if input.CronExpression != nil || input.Timezone != nil {
			return fmt.Errorf("%w: cron fields are only valid for cron task", ErrInvalidConfig)
		}
	default:
		return fmt.Errorf("%w: schedule_type must be CRON or DELAYED", ErrInvalidConfig)
	}
	return nil
}

// normalizeCreateInput 统一清理字符串并补齐 Worker 默认值。
func normalizeCreateInput(input *CreateInput) {
	input.Name = strings.TrimSpace(input.Name)
	input.Description = strings.TrimSpace(input.Description)
	input.ScheduleType = strings.ToUpper(strings.TrimSpace(input.ScheduleType))
	input.HandlerType = strings.TrimSpace(input.HandlerType)
	if input.CronExpression != nil {
		value := strings.TrimSpace(*input.CronExpression)
		input.CronExpression = &value
	}
	if input.Timezone != nil {
		value := strings.TrimSpace(*input.Timezone)
		input.Timezone = &value
	}
	if input.Concurrency == 0 {
		input.Concurrency = 1
	}
	if input.WorkerStatus == "" {
		input.WorkerStatus = WorkerStopped
	} else {
		input.WorkerStatus = strings.ToUpper(strings.TrimSpace(input.WorkerStatus))
	}
}

// enqueueDelayed 把延时任务以确定性 Task ID 写入专属队列，和 Cron 任务不同：不会经过 `admin_task_dispatch` 调度队列
func (r *Runtime) enqueueDelayed(definition *Definition) error {
	if definition.ExecuteAtMS == nil {
		return fmt.Errorf("%w: execute_at_ms is missing", ErrInvalidConfig)
	}
	executionID := delayedTaskID(definition.ID)
	payload, err := marshalEnvelope(*definition, executionID)
	if err != nil {
		return err
	}
	_, err = r.client.Enqueue(
		asynq.NewTask(asynqTaskType, payload),
		asynq.Queue(queueName(definition.ID)),
		asynq.MaxRetry(definition.Worker.MaxRetry),
		asynq.ProcessAt(time.UnixMilli(*definition.ExecuteAtMS)),
		asynq.TaskID(executionID),
	)
	if err != nil {
		return fmt.Errorf("enqueue delayed task: %w", err)
	}
	return nil
}

// handler 按稳定名称读取已注册业务处理器。
func (r *Runtime) handler(handlerType string) (Handler, bool) {
	r.registryMu.RLock()
	defer r.registryMu.RUnlock()
	handler, ok := r.handlers[handlerType]
	return handler, ok
}

// signalReconcile 非阻塞通知当前 Leader 尽快同步 Worker 状态。
func (r *Runtime) signalReconcile() {
	select {
	case r.reconcileCh <- struct{}{}:
	default:
	}
}

// queueName 根据任务定义主键生成隔离且可预测的 Asynq 队列名。
func queueName(taskID uint64) string {
	return fmt.Sprintf("admin_task_%d", taskID)
}

// delayedTaskID 根据任务定义主键生成延时任务的确定性 Asynq Task ID。
func delayedTaskID(taskID uint64) string {
	return fmt.Sprintf("admin-task-definition-%d", taskID)
}

// cronSpec 为任务表达式补充独立时区，@every 按持续时间执行无需时区。
func cronSpec(definition Definition) string {
	expression := strings.TrimSpace(*definition.CronExpression)
	if strings.HasPrefix(expression, "@every ") {
		return expression
	}
	return fmt.Sprintf("CRON_TZ=%s %s", strings.TrimSpace(*definition.Timezone), expression)
}

type taskEnvelope struct {
	ExecutionID      string          `json:"execution_id"`
	TaskDefinitionID uint64          `json:"task_definition_id"`
	Payload          json.RawMessage `json:"payload"`
}

// marshalEnvelope 固化本次入队使用的任务主键和 payload 快照。
func marshalEnvelope(definition Definition, executionID string) ([]byte, error) {
	payload, err := json.Marshal(taskEnvelope{
		ExecutionID:      executionID,
		TaskDefinitionID: definition.ID,
		Payload:          definition.Payload,
	})
	if err != nil {
		return nil, fmt.Errorf("marshal task envelope: %w", err)
	}
	return payload, nil
}
