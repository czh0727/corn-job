package task

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/hibiken/asynq"
	"github.com/redis/go-redis/v9"
)

type runtimeTestStore struct {
	taskStore
	mu                     sync.Mutex
	createdInput           CreateInput
	runningWorkers         []Worker
	finishedDelayedWorkers []Worker
	workerStatuses         []string
	definition             *Definition
	execution              Execution
	statuses               []string
}

// CreateTask 记录测试创建参数并返回可断言的任务定义。
func (s *runtimeTestStore) CreateTask(_ context.Context, input CreateInput, nowMS int64) (*Definition, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.createdInput = input
	return &Definition{
		ID:             1,
		Name:           input.Name,
		Description:    input.Description,
		ScheduleType:   input.ScheduleType,
		CronExpression: input.CronExpression,
		Timezone:       input.Timezone,
		ExecuteAtMS:    input.ExecuteAtMS,
		Payload:        input.Payload,
		Status:         DefinitionDraft,
		CreatedAtMS:    nowMS,
		UpdatedAtMS:    nowMS,
		Worker: Worker{
			TaskDefinitionID: 1,
			HandlerType:      input.HandlerType,
			Concurrency:      input.Concurrency,
			MaxRetry:         input.MaxRetry,
			Status:           input.WorkerStatus,
		},
	}, nil
}

// ListRunningTaskWorkers 返回测试预设的运行中 Worker。
func (s *runtimeTestStore) ListRunningTaskWorkers(context.Context) ([]Worker, error) {
	return s.runningWorkers, nil
}

// ListFinishedDelayedTaskWorkers 返回测试预设的待自动停止 Worker。
func (s *runtimeTestStore) ListFinishedDelayedTaskWorkers(context.Context) ([]Worker, error) {
	return s.finishedDelayedWorkers, nil
}

// TaskByID 返回测试预设的任务定义。
func (s *runtimeTestStore) TaskByID(context.Context, uint64) (*Definition, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.definition == nil {
		return nil, ErrNotFound
	}
	copy := *s.definition
	return &copy, nil
}

// StartTaskExecution 模拟执行中间件首次写入 RUNNING。
func (s *runtimeTestStore) StartTaskExecution(_ context.Context, executionID string, taskID uint64, nowMS int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.execution.Status == ExecutionCanceled {
		return nil
	}
	s.execution = Execution{ID: executionID, TaskDefinitionID: taskID, Status: ExecutionRunning, StartedAtMS: nowMS}
	s.statuses = append(s.statuses, ExecutionRunning)
	return nil
}

// FinishTaskExecution 模拟执行中间件更新最终或重试状态。
func (s *runtimeTestStore) FinishTaskExecution(_ context.Context, executionID, status string, errorMessage *string, finishedAtMS *int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.execution.ID = executionID
	s.execution.Status = status
	s.execution.ErrorMessage = errorMessage
	s.execution.FinishedAtMS = finishedAtMS
	s.statuses = append(s.statuses, status)
	return nil
}

// ExecutionByID 返回测试内存中的执行日志。
func (s *runtimeTestStore) ExecutionByID(context.Context, string) (*Execution, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	copy := s.execution
	return &copy, nil
}

// UpdateTaskStatus 更新测试任务的定义状态。
func (s *runtimeTestStore) UpdateTaskStatus(_ context.Context, _ uint64, status string, nowMS int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.definition != nil {
		s.definition.Status = status
		s.definition.UpdatedAtMS = nowMS
	}
	return nil
}

// UpdateTaskWorkerStatus 记录自动停止 Worker 的状态变更。
func (s *runtimeTestStore) UpdateTaskWorkerStatus(_ context.Context, _ uint64, status string, nowMS int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.workerStatuses = append(s.workerStatuses, status)
	if s.definition != nil {
		s.definition.Worker.Status = status
		s.definition.Worker.UpdatedAtMS = nowMS
	}
	return nil
}

type runtimeTestHandler struct{}

// ValidatePayload 接受包含 ok 字段的测试 payload。
func (runtimeTestHandler) ValidatePayload(raw json.RawMessage) error {
	var payload struct {
		OK bool `json:"ok"`
	}
	if err := json.Unmarshal(raw, &payload); err != nil {
		return err
	}
	if !payload.OK {
		return errors.New("ok must be true")
	}
	return nil
}

// Execute 为任务注册测试提供空业务实现。
func (runtimeTestHandler) Execute(context.Context, Job) error { return nil }

type runtimeFailingTestHandler struct{ runtimeTestHandler }

// Execute 模拟业务处理最终失败。
func (runtimeFailingTestHandler) Execute(context.Context, Job) error {
	return errors.New("test failure")
}

// TestRuntimeCreateValidatesAsynqCron 验证 Cron 格式由 Asynq 接受且六段式被框架拒绝。
func TestRuntimeCreateValidatesAsynqCron(t *testing.T) {
	runtime, cleanup := newRuntimeForTest(t, &runtimeTestStore{})
	defer cleanup()
	if err := runtime.Register("TEST", runtimeTestHandler{}); err != nil {
		t.Fatalf("Register() error = %v", err)
	}

	expression := "*/5 * * * *"
	timezone := "Asia/Shanghai"
	definition, err := runtime.Create(context.Background(), CreateInput{
		Name:           "五分钟任务",
		ScheduleType:   ScheduleCron,
		CronExpression: &expression,
		Timezone:       &timezone,
		Payload:        json.RawMessage(`{"ok":true}`),
		HandlerType:    "TEST",
		Concurrency:    1,
		MaxRetry:       3,
		WorkerStatus:   WorkerStopped,
	})
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if definition.ScheduleType != ScheduleCron {
		t.Fatalf("ScheduleType = %q, want %q", definition.ScheduleType, ScheduleCron)
	}

	sixFields := "*/5 * * * * *"
	_, err = runtime.Create(context.Background(), CreateInput{
		Name:           "非法六段任务",
		ScheduleType:   ScheduleCron,
		CronExpression: &sixFields,
		Timezone:       &timezone,
		Payload:        json.RawMessage(`{"ok":true}`),
		HandlerType:    "TEST",
		Concurrency:    1,
		MaxRetry:       3,
		WorkerStatus:   WorkerStopped,
	})
	if !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("Create() error = %v, want ErrInvalidConfig", err)
	}
}

// TestRuntimeCreateAcceptsSecondInterval 验证秒级任务使用 @every 且不保存无用时区。
func TestRuntimeCreateAcceptsSecondInterval(t *testing.T) {
	runtime, cleanup := newRuntimeForTest(t, &runtimeTestStore{})
	defer cleanup()
	if err := runtime.Register("TEST", runtimeTestHandler{}); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	expression := "@every 5s"
	_, err := runtime.Create(context.Background(), CreateInput{
		Name:           "五秒任务",
		ScheduleType:   ScheduleCron,
		CronExpression: &expression,
		Payload:        json.RawMessage(`{"ok":true}`),
		HandlerType:    "TEST",
		Concurrency:    1,
		MaxRetry:       0,
		WorkerStatus:   WorkerStopped,
	})
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
}

// TestRuntimeCreateLimitsTotalConcurrency 验证运行中 Worker 总并发不能超过框架硬上限。
func TestRuntimeCreateLimitsTotalConcurrency(t *testing.T) {
	store := &runtimeTestStore{runningWorkers: []Worker{{TaskDefinitionID: 10, Concurrency: MaxTotalConcurrency}}}
	runtime, cleanup := newRuntimeForTest(t, store)
	defer cleanup()
	if err := runtime.Register("TEST", runtimeTestHandler{}); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	executeAt := time.Now().Add(time.Hour).UnixMilli()
	_, err := runtime.Create(context.Background(), CreateInput{
		Name:         "并发超限任务",
		ScheduleType: ScheduleDelayed,
		ExecuteAtMS:  &executeAt,
		Payload:      json.RawMessage(`{"ok":true}`),
		HandlerType:  "TEST",
		Concurrency:  1,
		MaxRetry:     3,
		WorkerStatus: WorkerRunning,
	})
	if !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("Create() error = %v, want ErrInvalidConfig", err)
	}
}

// TestProcessTaskRecordsSuccessfulExecution 验证通用执行中间件记录 RUNNING 和 SUCCEEDED。
func TestProcessTaskRecordsSuccessfulExecution(t *testing.T) {
	store := &runtimeTestStore{definition: &Definition{
		ID:           7,
		ScheduleType: ScheduleCron,
		Status:       DefinitionActive,
		Payload:      json.RawMessage(`{"ok":true}`),
		Worker: Worker{
			TaskDefinitionID: 7,
			HandlerType:      "TEST",
			Concurrency:      1,
			MaxRetry:         1,
			Status:           WorkerRunning,
		},
	}}
	runtime, cleanup := newRuntimeForTest(t, store)
	defer cleanup()
	if err := runtime.Register("TEST", runtimeTestHandler{}); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	queue := queueName(7)
	server := asynq.NewServerFromRedisClient(runtime.redis, asynq.Config{
		Concurrency:       1,
		Queues:            map[string]int{queue: 1},
		TaskCheckInterval: 10 * time.Millisecond,
		ShutdownTimeout:   time.Second,
	})
	if err := server.Start(asynq.HandlerFunc(runtime.processTask)); err != nil {
		t.Fatalf("server.Start() error = %v", err)
	}
	defer func() {
		server.Stop()
		server.Shutdown()
	}()
	payload, err := marshalEnvelope(*store.definition, "exec-test")
	if err != nil {
		t.Fatalf("marshalEnvelope() error = %v", err)
	}
	if _, err := runtime.client.Enqueue(
		asynq.NewTask(asynqTaskType, payload),
		asynq.Queue(queue),
		asynq.TaskID("exec-test"),
	); err != nil {
		t.Fatalf("Enqueue() error = %v", err)
	}
	waitForTest(t, func() bool {
		store.mu.Lock()
		defer store.mu.Unlock()
		return store.execution.Status == ExecutionSucceeded
	})
	store.mu.Lock()
	statuses := append([]string(nil), store.statuses...)
	store.mu.Unlock()
	if len(statuses) < 2 || statuses[0] != ExecutionRunning || statuses[len(statuses)-1] != ExecutionSucceeded {
		t.Fatalf("execution statuses = %#v", statuses)
	}
}

// TestDelayedExecutionOnlyScheduledRunFinishesDefinition 验证手动执行不会提前结束一次性任务。
func TestDelayedExecutionOnlyScheduledRunFinishesDefinition(t *testing.T) {
	tests := []struct {
		name               string
		executionID        string
		handler            Handler
		wantExecutionState string
		wantTaskState      string
	}{
		{name: "scheduled success", executionID: delayedTaskID(7), handler: runtimeTestHandler{}, wantExecutionState: ExecutionSucceeded, wantTaskState: DefinitionFinished},
		{name: "scheduled final failure", executionID: delayedTaskID(7), handler: runtimeFailingTestHandler{}, wantExecutionState: ExecutionFailed, wantTaskState: DefinitionFinished},
		{name: "manual", executionID: "exec-manual", handler: runtimeTestHandler{}, wantExecutionState: ExecutionSucceeded, wantTaskState: DefinitionActive},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := &runtimeTestStore{definition: &Definition{
				ID:           7,
				ScheduleType: ScheduleDelayed,
				Status:       DefinitionActive,
				Payload:      json.RawMessage(`{"ok":true}`),
				Worker: Worker{
					TaskDefinitionID: 7,
					HandlerType:      "TEST",
					Concurrency:      1,
					MaxRetry:         0,
					Status:           WorkerRunning,
				},
			}}
			runtime, cleanup := newRuntimeForTest(t, store)
			defer cleanup()
			if err := runtime.Register("TEST", tt.handler); err != nil {
				t.Fatalf("Register() error = %v", err)
			}
			queue := queueName(7)
			server := asynq.NewServerFromRedisClient(runtime.redis, asynq.Config{
				Concurrency:       1,
				Queues:            map[string]int{queue: 1},
				TaskCheckInterval: 10 * time.Millisecond,
				ShutdownTimeout:   time.Second,
			})
			if err := server.Start(asynq.HandlerFunc(runtime.processTask)); err != nil {
				t.Fatalf("server.Start() error = %v", err)
			}
			defer func() {
				server.Stop()
				server.Shutdown()
			}()
			payload, err := marshalEnvelope(*store.definition, tt.executionID)
			if err != nil {
				t.Fatalf("marshalEnvelope() error = %v", err)
			}
			if _, err := runtime.client.Enqueue(
				asynq.NewTask(asynqTaskType, payload),
				asynq.Queue(queue),
				asynq.TaskID(tt.executionID),
				asynq.MaxRetry(0),
			); err != nil {
				t.Fatalf("Enqueue() error = %v", err)
			}
			waitForTest(t, func() bool {
				store.mu.Lock()
				defer store.mu.Unlock()
				return store.execution.Status == tt.wantExecutionState
			})
			store.mu.Lock()
			gotTaskState := store.definition.Status
			store.mu.Unlock()
			if gotTaskState != tt.wantTaskState {
				t.Fatalf("task status = %q, want %q", gotTaskState, tt.wantTaskState)
			}
		})
	}
}

// TestReconcileStopsFinishedDelayedWorkerOnEmptyQueue 验证空队列的一次性 Worker 会自动退出。
func TestReconcileStopsFinishedDelayedWorkerOnEmptyQueue(t *testing.T) {
	worker := Worker{TaskDefinitionID: 7, HandlerType: "TEST", Concurrency: 1, Status: WorkerRunning}
	store := &runtimeTestStore{
		runningWorkers:         []Worker{worker},
		finishedDelayedWorkers: []Worker{worker},
	}
	runtime, cleanup := newRuntimeForTest(t, store)
	defer cleanup()
	running := map[uint64]*runningWorker{7: {config: worker}}
	if err := runtime.reconcileWorkers(context.Background(), running); err != nil {
		t.Fatalf("reconcileWorkers() error = %v", err)
	}
	if len(running) != 0 {
		t.Fatalf("running workers = %d, want 0", len(running))
	}
	store.mu.Lock()
	statuses := append([]string(nil), store.workerStatuses...)
	store.mu.Unlock()
	if len(statuses) != 1 || statuses[0] != WorkerStopped {
		t.Fatalf("worker statuses = %#v, want STOPPED", statuses)
	}
}

// TestReconcileKeepsFinishedDelayedWorkerUntilRunnableQueueDrains 验证队列未清空前不会提前关闭 Worker。
func TestReconcileKeepsFinishedDelayedWorkerUntilRunnableQueueDrains(t *testing.T) {
	worker := Worker{TaskDefinitionID: 7, HandlerType: "TEST", Concurrency: 1, Status: WorkerRunning}
	store := &runtimeTestStore{
		runningWorkers:         []Worker{worker},
		finishedDelayedWorkers: []Worker{worker},
	}
	runtime, cleanup := newRuntimeForTest(t, store)
	defer cleanup()
	if _, err := runtime.client.Enqueue(
		asynq.NewTask(asynqTaskType, nil),
		asynq.Queue(queueName(7)),
		asynq.ProcessAt(time.Now().Add(time.Hour)),
	); err != nil {
		t.Fatalf("Enqueue() error = %v", err)
	}
	running := map[uint64]*runningWorker{7: {config: worker}}
	if err := runtime.reconcileWorkers(context.Background(), running); err != nil {
		t.Fatalf("reconcileWorkers() error = %v", err)
	}
	if len(running) != 1 {
		t.Fatalf("running workers = %d, want 1", len(running))
	}
	store.mu.Lock()
	statuses := append([]string(nil), store.workerStatuses...)
	store.mu.Unlock()
	if len(statuses) != 0 {
		t.Fatalf("worker statuses = %#v, want no automatic stop", statuses)
	}
}

// TestReconcileStartsWorkerWhenQueueDoesNotExist 验证首次入队前不存在的队列不会阻止 Worker 启动。
func TestReconcileStartsWorkerWhenQueueDoesNotExist(t *testing.T) {
	worker := Worker{TaskDefinitionID: 7, HandlerType: "TEST", Concurrency: 1, Status: WorkerRunning}
	definition := &Definition{
		ID:           7,
		ScheduleType: ScheduleCron,
		Status:       DefinitionActive,
		Payload:      json.RawMessage(`{"ok":true}`),
		Worker:       worker,
	}
	store := &runtimeTestStore{runningWorkers: []Worker{worker}, definition: definition}
	runtime, cleanup := newRuntimeForTest(t, store)
	defer cleanup()
	if err := runtime.Register("TEST", runtimeTestHandler{}); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	running := make(map[uint64]*runningWorker)
	defer runtime.stopAllWorkers(running)

	if err := runtime.reconcileWorkers(context.Background(), running); err != nil {
		t.Fatalf("reconcileWorkers() error = %v", err)
	}
	if len(running) != 1 {
		t.Fatalf("running workers = %d, want 1", len(running))
	}
	executionID := "exec-first-enqueue"
	payload, err := marshalEnvelope(*definition, executionID)
	if err != nil {
		t.Fatalf("marshalEnvelope() error = %v", err)
	}
	if _, err := runtime.client.Enqueue(
		asynq.NewTask(asynqTaskType, payload),
		asynq.Queue(queueName(worker.TaskDefinitionID)),
		asynq.TaskID(executionID),
	); err != nil {
		t.Fatalf("Enqueue() error = %v", err)
	}
	waitForTest(t, func() bool {
		store.mu.Lock()
		defer store.mu.Unlock()
		return store.execution.Status == ExecutionSucceeded
	})
}

// TestStopWorkerWhenQueueDoesNotExist 验证从未入队的 Worker 可以正常停止。
func TestStopWorkerWhenQueueDoesNotExist(t *testing.T) {
	store := &runtimeTestStore{definition: &Definition{
		ID: 7,
		Worker: Worker{
			TaskDefinitionID: 7,
			HandlerType:      "TEST",
			Concurrency:      1,
			Status:           WorkerRunning,
		},
	}}
	runtime, cleanup := newRuntimeForTest(t, store)
	defer cleanup()

	if err := runtime.StopWorker(context.Background(), 7); err != nil {
		t.Fatalf("StopWorker() error = %v", err)
	}
	store.mu.Lock()
	statuses := append([]string(nil), store.workerStatuses...)
	store.mu.Unlock()
	if len(statuses) != 1 || statuses[0] != WorkerStopped {
		t.Fatalf("worker statuses = %#v, want STOPPED", statuses)
	}
}

// TestReconcileUnpausesExistingQueue 验证已有的暂停队列仍会在 Worker 启动时恢复。
func TestReconcileUnpausesExistingQueue(t *testing.T) {
	worker := Worker{TaskDefinitionID: 7, HandlerType: "TEST", Concurrency: 1, Status: WorkerRunning}
	store := &runtimeTestStore{runningWorkers: []Worker{worker}}
	runtime, cleanup := newRuntimeForTest(t, store)
	defer cleanup()
	queue := queueName(worker.TaskDefinitionID)
	if _, err := runtime.client.Enqueue(
		asynq.NewTask(asynqTaskType, nil),
		asynq.Queue(queue),
		asynq.ProcessAt(time.Now().Add(time.Hour)),
	); err != nil {
		t.Fatalf("Enqueue() error = %v", err)
	}
	if err := runtime.inspector.PauseQueue(queue); err != nil {
		t.Fatalf("PauseQueue() error = %v", err)
	}
	running := make(map[uint64]*runningWorker)
	defer runtime.stopAllWorkers(running)

	if err := runtime.reconcileWorkers(context.Background(), running); err != nil {
		t.Fatalf("reconcileWorkers() error = %v", err)
	}
	info, err := runtime.inspector.GetQueueInfo(queue)
	if err != nil {
		t.Fatalf("GetQueueInfo() error = %v", err)
	}
	if info.Paused {
		t.Fatal("queue is still paused after reconcile")
	}
}

// TestRunTaskRejectsFinishedDefinition 验证一次性任务结束后不能再次手动入队。
func TestRunTaskRejectsFinishedDefinition(t *testing.T) {
	store := &runtimeTestStore{definition: &Definition{ID: 7, Status: DefinitionFinished}}
	runtime, cleanup := newRuntimeForTest(t, store)
	defer cleanup()
	if _, err := runtime.RunTask(context.Background(), 7); !errors.Is(err, ErrConflict) {
		t.Fatalf("RunTask() error = %v, want ErrConflict", err)
	}
}

// TestSchedulerUsesOwnedRedisConnection 验证 Scheduler 关闭时不会尝试关闭 Runtime 的共享连接。
func TestSchedulerUsesOwnedRedisConnection(t *testing.T) {
	runtime, cleanup := newRuntimeForTest(t, &runtimeTestStore{})
	defer cleanup()
	scheduler := asynq.NewScheduler(runtime.schedulerRedis, &asynq.SchedulerOpts{LogLevel: asynq.ErrorLevel})
	if err := scheduler.Start(); err != nil {
		t.Fatalf("scheduler.Start() error = %v", err)
	}
	scheduler.Shutdown()
	if err := runtime.redis.Ping(context.Background()).Err(); err != nil {
		t.Fatalf("shared redis connection was closed: %v", err)
	}
}

// newRuntimeForTest 创建使用独立内存 Redis 的任务运行时测试实例。
func newRuntimeForTest(t *testing.T, store taskStore) (*Runtime, func()) {
	t.Helper()
	mini := miniredis.RunT(t)
	redisClient := redis.NewClient(&redis.Options{Addr: mini.Addr()})
	runtime := NewRuntime(store, redisClient)
	cleanup := func() {
		_ = redisClient.Close()
		mini.Close()
	}
	return runtime, cleanup
}

// waitForTest 轮询异步 Asynq 断言直到成功或测试超时。
func waitForTest(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("condition was not met before timeout")
}
