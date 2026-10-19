package task

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"runtime/debug"
	"time"
	"unicode/utf8"

	"github.com/hibiken/asynq"
)

type triggerEnvelope struct {
	TaskDefinitionID uint64 `json:"task_definition_id"`
	RuntimeEpoch     int64  `json:"runtime_epoch"`
}

// processDispatch 校验 Leader epoch 和任务状态后，把 Cron Trigger 转成带 payload 快照的业务 Job。
func (r *Runtime) processDispatch(ctx context.Context, trigger *asynq.Task) error {
	var envelope triggerEnvelope
	if err := json.Unmarshal(trigger.Payload(), &envelope); err != nil || envelope.TaskDefinitionID == 0 || envelope.RuntimeEpoch <= 0 {
		return fmt.Errorf("%w: invalid cron trigger", asynq.SkipRetry)
	}
	currentEpoch, err := r.redis.Get(ctx, runtimeEpochKey).Int64()
	if err != nil {
		return fmt.Errorf("read task runtime epoch: %w", err)
	}
	if currentEpoch != envelope.RuntimeEpoch {
		return nil
	}
	triggerID, ok := asynq.GetTaskID(ctx)
	if !ok || triggerID == "" {
		return fmt.Errorf("%w: cron trigger id is missing", asynq.SkipRetry)
	}
	definition, err := r.store.TaskByID(ctx, envelope.TaskDefinitionID)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return nil
		}
		return fmt.Errorf("load cron task definition: %w", err)
	}
	if definition.Status != DefinitionActive {
		return nil
	}
	// 发任务前要有实现好的handler
	handler, exists := r.handler(definition.Worker.HandlerType)
	if !exists {
		return fmt.Errorf("%w: handler %q is not registered", asynq.SkipRetry, definition.Worker.HandlerType)
	}
	if err := handler.ValidatePayload(definition.Payload); err != nil {
		return fmt.Errorf("%w: validate cron task payload: %v", asynq.SkipRetry, err)
	}
	executionID := "exec-" + triggerID
	payload, err := marshalEnvelope(*definition, executionID)
	if err != nil {
		return err
	}
	_, err = r.client.Enqueue(
		asynq.NewTask(asynqTaskType, payload),
		asynq.Queue(queueName(definition.ID)),
		asynq.TaskID(executionID),
		asynq.MaxRetry(definition.Worker.MaxRetry),
	)
	if errors.Is(err, asynq.ErrTaskIDConflict) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("enqueue cron business task: %w", err)
	}
	return nil
}

// processTask 作为通用执行中间件，统一维护日志状态并调用注册的业务处理器。
func (r *Runtime) processTask(ctx context.Context, asynqTask *asynq.Task) (resultErr error) {
	var envelope taskEnvelope
	if err := json.Unmarshal(asynqTask.Payload(), &envelope); err != nil || envelope.TaskDefinitionID == 0 || envelope.ExecutionID == "" {
		return fmt.Errorf("%w: invalid task envelope", asynq.SkipRetry)
	}
	executionID, ok := asynq.GetTaskID(ctx)
	if !ok || executionID == "" || executionID != envelope.ExecutionID {
		return fmt.Errorf("%w: asynq task id does not match execution id", asynq.SkipRetry)
	}
	if err := r.store.StartTaskExecution(ctx, executionID, envelope.TaskDefinitionID, time.Now().UnixMilli()); err != nil {
		return fmt.Errorf("start task execution log: %w", err)
	}
	execution, err := r.store.ExecutionByID(ctx, executionID)
	if err != nil {
		return fmt.Errorf("reload task execution log: %w", err)
	}
	if execution.Status == ExecutionCanceled {
		return nil
	}
	var definition *Definition
	defer func() {
		if recovered := recover(); recovered != nil {
			panicErr := fmt.Errorf("business handler panic: %v", recovered)
			r.log.ErrorContext(ctx, "task handler panic", "execution_id", executionID, "panic", recovered, "stack", string(debug.Stack()))
			resultErr = r.finishTaskError(ctx, definition, executionID, panicErr)
		}
	}()

	definition, err = r.store.TaskByID(ctx, envelope.TaskDefinitionID)
	if err != nil {
		return r.finishWithError(ctx, executionID, err)
	}
	handler, ok := r.handler(definition.Worker.HandlerType)
	if !ok {
		return r.finishTaskError(ctx, definition, executionID, Permanent(fmt.Errorf("handler %q is not registered", definition.Worker.HandlerType)))
	}
	if err := handler.ValidatePayload(envelope.Payload); err != nil {
		return r.finishTaskError(ctx, definition, executionID, Permanent(fmt.Errorf("validate payload: %w", err)))
	}
	if err := handler.Execute(ctx, Job{
		ExecutionID:      executionID,
		TaskDefinitionID: envelope.TaskDefinitionID,
		Payload:          envelope.Payload,
	}); err != nil {
		return r.finishTaskError(ctx, definition, executionID, err)
	}

	finishedAt := time.Now().UnixMilli()
	if err := r.store.FinishTaskExecution(ctx, executionID, ExecutionSucceeded, nil, &finishedAt); err != nil {
		return fmt.Errorf("finish successful task execution log: %w", err)
	}
	if definition.ScheduleType == ScheduleDelayed && executionID == delayedTaskID(definition.ID) {
		if err := r.store.UpdateTaskStatus(ctx, definition.ID, DefinitionFinished, finishedAt); err != nil {
			r.log.WarnContext(ctx, "finish delayed task definition failed", "task_id", definition.ID, "error", err)
		} else {
			r.signalReconcile()
		}
	}
	return nil
}

// finishTaskError 完成失败状态，并在延时任务达到最终结果后关闭任务定义。
func (r *Runtime) finishTaskError(ctx context.Context, definition *Definition, executionID string, taskErr error) error {
	resultErr := r.finishWithError(ctx, executionID, taskErr)
	if definition != nil && definition.ScheduleType == ScheduleDelayed && executionID == delayedTaskID(definition.ID) && errors.Is(resultErr, asynq.SkipRetry) {
		finishedAt := time.Now().UnixMilli()
		if err := r.store.UpdateTaskStatus(ctx, definition.ID, DefinitionFinished, finishedAt); err != nil {
			r.log.WarnContext(ctx, "finish failed delayed task definition failed", "task_id", definition.ID, "error", err)
		} else {
			r.signalReconcile()
		}
	}
	return resultErr
}

// finishWithError 根据 Asynq 重试上下文把失败记录为 RETRYING 或最终 FAILED。
func (r *Runtime) finishWithError(ctx context.Context, executionID string, taskErr error) error {
	retried, _ := asynq.GetRetryCount(ctx)
	maxRetry, _ := asynq.GetMaxRetry(ctx)
	message := truncateError(taskErr, 2000)
	if IsPermanent(taskErr) || retried >= maxRetry {
		finishedAt := time.Now().UnixMilli()
		if err := r.store.FinishTaskExecution(ctx, executionID, ExecutionFailed, &message, &finishedAt); err != nil {
			return fmt.Errorf("finish failed task execution log: %w", err)
		}
		return fmt.Errorf("%w: %v", asynq.SkipRetry, taskErr)
	}
	if err := r.store.FinishTaskExecution(ctx, executionID, ExecutionRetrying, &message, nil); err != nil {
		return fmt.Errorf("mark retrying task execution log: %w", err)
	}
	return taskErr
}

// truncateError 按字符边界截断失败原因，确保能够写入 VARCHAR(2000)。
func truncateError(err error, maxBytes int) string {
	if err == nil || maxBytes <= 0 {
		return ""
	}
	message := err.Error()
	if len(message) <= maxBytes {
		return message
	}
	for len(message) > maxBytes {
		_, size := utf8.DecodeLastRuneInString(message)
		if size == 0 {
			return ""
		}
		message = message[:len(message)-size]
	}
	return message
}
