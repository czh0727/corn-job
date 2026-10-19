package handler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"

	tasksvc "corn-job/service/task"
	"github.com/gin-gonic/gin"
)

// Task 提供任务定义、Worker 启停和执行日志管理接口。
type Task struct {
	runtime *tasksvc.Runtime
}

type createTaskRequest struct {
	Name           string              `json:"name" binding:"required"`
	Description    string              `json:"description"`
	ScheduleType   string              `json:"schedule_type" binding:"required"`
	CronExpression *string             `json:"cron_expression"`
	Timezone       *string             `json:"timezone"`
	ExecuteAtMS    *int64              `json:"execute_at_ms"`
	Payload        json.RawMessage     `json:"payload" binding:"required"`
	Worker         createWorkerRequest `json:"worker" binding:"required"`
}

type createWorkerRequest struct {
	HandlerType string `json:"handler_type" binding:"required"`
	Concurrency int    `json:"concurrency"`
	MaxRetry    *int   `json:"max_retry"`
	Status      string `json:"status"`
}

type updateTaskRequest struct {
	Name           *string          `json:"name"`
	Description    *string          `json:"description"`
	ScheduleType   *string          `json:"schedule_type"`
	CronExpression *string          `json:"cron_expression"`
	Timezone       *string          `json:"timezone"`
	ExecuteAtMS    *int64           `json:"execute_at_ms"`
	Payload        *json.RawMessage `json:"payload"`
}

type updateWorkerRequest struct {
	HandlerType *string `json:"handler_type"`
	Concurrency *int    `json:"concurrency"`
	MaxRetry    *int    `json:"max_retry"`
}

type taskListResponse struct {
	Items []tasksvc.Definition `json:"items"`
}

type taskExecutionListResponse struct {
	Items []tasksvc.Execution `json:"items"`
	Total int64               `json:"total"`
	Page  int                 `json:"page"`
	Size  int                 `json:"page_size"`
}

type runTaskResponse struct {
	ExecutionID string `json:"execution_id"`
}

// NewTask 创建通用任务管理 Handler。
func NewTask(runtime *tasksvc.Runtime) *Task {
	return &Task{runtime: runtime}
}

// Create 创建任务定义和它唯一对应的 Worker 配置。
func (h *Task) Create(c *gin.Context) {
	if !h.ready(c) {
		return
	}
	var req createTaskRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		handleTaskError(c, fmt.Errorf("%w: request body: %v", tasksvc.ErrInvalidConfig, err))
		return
	}
	maxRetry := 3
	if req.Worker.MaxRetry != nil {
		maxRetry = *req.Worker.MaxRetry
	}
	definition, err := h.runtime.Create(c.Request.Context(), tasksvc.CreateInput{
		Name:           req.Name,
		Description:    req.Description,
		ScheduleType:   req.ScheduleType,
		CronExpression: req.CronExpression,
		Timezone:       req.Timezone,
		ExecuteAtMS:    req.ExecuteAtMS,
		Payload:        req.Payload,
		HandlerType:    req.Worker.HandlerType,
		Concurrency:    req.Worker.Concurrency,
		MaxRetry:       maxRetry,
		WorkerStatus:   req.Worker.Status,
	})
	if err != nil {
		handleTaskError(c, err)
		return
	}
	c.JSON(http.StatusCreated, definition)
}

// List 返回全部任务定义和 Worker 配置。
func (h *Task) List(c *gin.Context) {
	if !h.ready(c) {
		return
	}
	items, err := h.runtime.List(c.Request.Context())
	if err != nil {
		handleTaskError(c, err)
		return
	}
	c.JSON(http.StatusOK, taskListResponse{Items: items})
}

// Get 返回指定任务定义和 Worker 配置。
func (h *Task) Get(c *gin.Context) {
	if !h.ready(c) {
		return
	}
	id, ok := taskIDParam(c)
	if !ok {
		return
	}
	definition, err := h.runtime.Get(c.Request.Context(), id)
	if err != nil {
		handleTaskError(c, err)
		return
	}
	c.JSON(http.StatusOK, definition)
}

// Update 修改尚未发布或已经暂停的任务定义。
func (h *Task) Update(c *gin.Context) {
	if !h.ready(c) {
		return
	}
	id, ok := taskIDParam(c)
	if !ok {
		return
	}
	var req updateTaskRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		handleTaskError(c, fmt.Errorf("%w: request body: %v", tasksvc.ErrInvalidConfig, err))
		return
	}
	definition, err := h.runtime.Update(c.Request.Context(), id, tasksvc.UpdateInput{
		Name:           req.Name,
		Description:    req.Description,
		ScheduleType:   req.ScheduleType,
		CronExpression: req.CronExpression,
		Timezone:       req.Timezone,
		ExecuteAtMS:    req.ExecuteAtMS,
		Payload:        req.Payload,
	})
	if err != nil {
		handleTaskError(c, err)
		return
	}
	c.JSON(http.StatusOK, definition)
}

// Start 启用定时任务，或把延时任务写入 Asynq 计划队列。
func (h *Task) Start(c *gin.Context) {
	if !h.ready(c) {
		return
	}
	h.changeState(c, h.runtime.StartTask)
}

// Resume 恢复已经暂停的任务调度。
func (h *Task) Resume(c *gin.Context) {
	if !h.ready(c) {
		return
	}
	h.changeState(c, h.runtime.StartTask)
}

// Pause 暂停任务的后续调度和业务执行。
func (h *Task) Pause(c *gin.Context) {
	if !h.ready(c) {
		return
	}
	h.changeState(c, h.runtime.PauseTask)
}

// Run 立即投递一次当前任务并返回新的 Execution ID。
func (h *Task) Run(c *gin.Context) {
	if !h.ready(c) {
		return
	}
	id, ok := taskIDParam(c)
	if !ok {
		return
	}
	executionID, err := h.runtime.RunTask(c.Request.Context(), id)
	if err != nil {
		handleTaskError(c, err)
		return
	}
	c.JSON(http.StatusAccepted, runTaskResponse{ExecutionID: executionID})
}

// GetWorker 返回指定任务唯一对应的 Worker 配置。
func (h *Task) GetWorker(c *gin.Context) {
	if !h.ready(c) {
		return
	}
	id, ok := taskIDParam(c)
	if !ok {
		return
	}
	definition, err := h.runtime.Get(c.Request.Context(), id)
	if err != nil {
		handleTaskError(c, err)
		return
	}
	c.JSON(http.StatusOK, definition.Worker)
}

// UpdateWorker 修改任务 Worker 的处理器、并发或失败重试配置。
func (h *Task) UpdateWorker(c *gin.Context) {
	if !h.ready(c) {
		return
	}
	id, ok := taskIDParam(c)
	if !ok {
		return
	}
	var req updateWorkerRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		handleTaskError(c, fmt.Errorf("%w: request body: %v", tasksvc.ErrInvalidConfig, err))
		return
	}
	worker, err := h.runtime.UpdateWorker(c.Request.Context(), id, tasksvc.WorkerUpdateInput{
		HandlerType: req.HandlerType,
		Concurrency: req.Concurrency,
		MaxRetry:    req.MaxRetry,
	})
	if err != nil {
		handleTaskError(c, err)
		return
	}
	c.JSON(http.StatusOK, worker)
}

// StartWorker 启动任务专属 Worker 并恢复其 Asynq 队列消费。
func (h *Task) StartWorker(c *gin.Context) {
	if !h.ready(c) {
		return
	}
	h.changeState(c, h.runtime.StartWorker)
}

// StopWorker 暂停任务队列并停止任务专属 Worker。
func (h *Task) StopWorker(c *gin.Context) {
	if !h.ready(c) {
		return
	}
	h.changeState(c, h.runtime.StopWorker)
}

// ListExecutions 按可选任务 ID 和状态分页返回执行日志。
func (h *Task) ListExecutions(c *gin.Context) {
	if !h.ready(c) {
		return
	}
	var taskID *uint64
	if raw := c.Query("task_definition_id"); raw != "" {
		id, err := strconv.ParseUint(raw, 10, 64)
		if err != nil || id == 0 {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid task_definition_id"})
			return
		}
		taskID = &id
	}
	page := taskPositiveQueryInt(c.Query("page"), 1)
	size := taskPositiveQueryInt(c.Query("page_size"), 20)
	items, total, err := h.runtime.ListExecutions(c.Request.Context(), taskID, c.Query("status"), page, size)
	if err != nil {
		handleTaskError(c, err)
		return
	}
	c.JSON(http.StatusOK, taskExecutionListResponse{Items: items, Total: total, Page: page, Size: size})
}

// GetExecution 返回单条任务执行日志。
func (h *Task) GetExecution(c *gin.Context) {
	if !h.ready(c) {
		return
	}
	id := c.Param("id")
	if id == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid execution id"})
		return
	}
	execution, err := h.runtime.GetExecution(c.Request.Context(), id)
	if err != nil {
		handleTaskError(c, err)
		return
	}
	c.JSON(http.StatusOK, execution)
}

// CancelExecution 尽力取消正在执行或等待重试的任务。
func (h *Task) CancelExecution(c *gin.Context) {
	if !h.ready(c) {
		return
	}
	id := c.Param("id")
	if id == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid execution id"})
		return
	}
	if err := h.runtime.CancelExecution(c.Request.Context(), id); err != nil {
		handleTaskError(c, err)
		return
	}
	c.Status(http.StatusNoContent)
}

// changeState 统一处理任务和 Worker 启停接口的 ID 解析与错误响应。
func (h *Task) changeState(c *gin.Context, action func(context.Context, uint64) error) {
	id, ok := taskIDParam(c)
	if !ok {
		return
	}
	if err := action(c.Request.Context(), id); err != nil {
		handleTaskError(c, err)
		return
	}
	c.Status(http.StatusNoContent)
}

// ready 检查任务运行时是否已完成依赖注入。
func (h *Task) ready(c *gin.Context) bool {
	if h == nil || h.runtime == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "task runtime not ready"})
		return false
	}
	return true
}

// taskIDParam 解析任务路由主键并输出统一参数错误。
func taskIDParam(c *gin.Context) (uint64, bool) {
	id, err := strconv.ParseUint(c.Param("id"), 10, 64)
	if err != nil || id == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid task id"})
		return 0, false
	}
	return id, true
}

// taskPositiveQueryInt 解析正整数分页参数并在非法时使用默认值。
func taskPositiveQueryInt(raw string, fallback int) int {
	value, err := strconv.Atoi(raw)
	if err != nil || value <= 0 {
		return fallback
	}
	return value
}

// handleTaskError 把任务领域错误映射为 HTTP 错误响应。
func handleTaskError(c *gin.Context, err error) {
	switch {
	case errors.Is(err, tasksvc.ErrNotFound):
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
	case errors.Is(err, tasksvc.ErrConflict):
		c.JSON(http.StatusConflict, gin.H{"error": err.Error()})
	case errors.Is(err, tasksvc.ErrInvalidConfig):
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
	default:
		c.JSON(http.StatusInternalServerError, gin.H{"error": "internal server error"})
	}
}
