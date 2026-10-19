package db

import (
	"context"
	"errors"

	tasksvc "corn-job/service/task"
	"gorm.io/gorm"
)

type taskDefinitionRow struct {
	ID             uint64  `gorm:"column:id;primaryKey"`
	Name           string  `gorm:"column:name"`
	Description    string  `gorm:"column:description"`
	ScheduleType   string  `gorm:"column:schedule_type"`
	CronExpression *string `gorm:"column:cron_expression"`
	Timezone       *string `gorm:"column:timezone"`
	ExecuteAtMS    *int64  `gorm:"column:execute_at_ms"`
	Payload        []byte  `gorm:"column:payload"`
	Status         string  `gorm:"column:status"`
	CreatedAtMS    int64   `gorm:"column:created_at_ms"`
	UpdatedAtMS    int64   `gorm:"column:updated_at_ms"`
}

// TableName 返回任务定义表名。
func (taskDefinitionRow) TableName() string { return "admin_task_definitions" }

type taskWorkerRow struct {
	ID               uint64 `gorm:"column:id;primaryKey"`
	TaskDefinitionID uint64 `gorm:"column:task_definition_id"`
	HandlerType      string `gorm:"column:handler_type"`
	Concurrency      int    `gorm:"column:concurrency"`
	MaxRetry         int    `gorm:"column:max_retry"`
	Status           string `gorm:"column:status"`
	CreatedAtMS      int64  `gorm:"column:created_at_ms"`
	UpdatedAtMS      int64  `gorm:"column:updated_at_ms"`
}

// TableName 返回任务 Worker 配置表名。
func (taskWorkerRow) TableName() string { return "admin_task_workers" }

type taskExecutionRow struct {
	ID               string  `gorm:"column:id;primaryKey"`
	TaskDefinitionID uint64  `gorm:"column:task_definition_id"`
	Status           string  `gorm:"column:status"`
	ErrorMessage     *string `gorm:"column:error_message"`
	StartedAtMS      int64   `gorm:"column:started_at_ms"`
	FinishedAtMS     *int64  `gorm:"column:finished_at_ms"`
}

// TableName 返回任务执行日志表名。
func (taskExecutionRow) TableName() string { return "admin_task_executions" }

// CreateTask 在同一事务中创建任务定义及其唯一 Worker 配置。
func (s *Store) CreateTask(ctx context.Context, input tasksvc.CreateInput, nowMS int64) (*tasksvc.Definition, error) {
	definition := taskDefinitionRow{
		Name:           input.Name,
		Description:    input.Description,
		ScheduleType:   input.ScheduleType,
		CronExpression: input.CronExpression,
		Timezone:       input.Timezone,
		ExecuteAtMS:    input.ExecuteAtMS,
		Payload:        input.Payload,
		Status:         tasksvc.DefinitionDraft,
		CreatedAtMS:    nowMS,
		UpdatedAtMS:    nowMS,
	}
	worker := taskWorkerRow{
		HandlerType: input.HandlerType,
		Concurrency: input.Concurrency,
		MaxRetry:    input.MaxRetry,
		Status:      input.WorkerStatus,
		CreatedAtMS: nowMS,
		UpdatedAtMS: nowMS,
	}

	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Create(&definition).Error; err != nil {
			return err
		}
		worker.TaskDefinitionID = definition.ID
		return tx.Create(&worker).Error
	})
	if err != nil {
		return nil, err
	}
	result := taskDefinitionFromRows(definition, worker)
	return &result, nil
}

// ListTasks 返回全部任务定义及对应的 Worker 配置。
func (s *Store) ListTasks(ctx context.Context) ([]tasksvc.Definition, error) {
	var definitions []taskDefinitionRow
	if err := s.db.WithContext(ctx).Order("id DESC").Find(&definitions).Error; err != nil {
		return nil, err
	}
	if len(definitions) == 0 {
		return []tasksvc.Definition{}, nil
	}

	ids := make([]uint64, 0, len(definitions))
	for _, definition := range definitions {
		ids = append(ids, definition.ID)
	}
	var workers []taskWorkerRow
	if err := s.db.WithContext(ctx).Where("task_definition_id IN ?", ids).Find(&workers).Error; err != nil {
		return nil, err
	}
	workerByTask := make(map[uint64]taskWorkerRow, len(workers))
	for _, worker := range workers {
		workerByTask[worker.TaskDefinitionID] = worker
	}

	result := make([]tasksvc.Definition, 0, len(definitions))
	for _, definition := range definitions {
		result = append(result, taskDefinitionFromRows(definition, workerByTask[definition.ID]))
	}
	return result, nil
}

// TaskByID 按主键读取任务定义及其 Worker 配置。
func (s *Store) TaskByID(ctx context.Context, id uint64) (*tasksvc.Definition, error) {
	var definition taskDefinitionRow
	if err := s.db.WithContext(ctx).First(&definition, id).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, tasksvc.ErrNotFound
		}
		return nil, err
	}
	var worker taskWorkerRow
	if err := s.db.WithContext(ctx).Where("task_definition_id = ?", id).First(&worker).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, tasksvc.ErrNotFound
		}
		return nil, err
	}
	result := taskDefinitionFromRows(definition, worker)
	return &result, nil
}

// UpdateTaskDefinition 覆盖已经由任务服务合并并校验过的完整定义字段。
func (s *Store) UpdateTaskDefinition(ctx context.Context, definition tasksvc.Definition, nowMS int64) error {
	result := s.db.WithContext(ctx).Model(&taskDefinitionRow{}).Where("id = ?", definition.ID).
		Updates(map[string]any{
			"name":            definition.Name,
			"description":     definition.Description,
			"schedule_type":   definition.ScheduleType,
			"cron_expression": definition.CronExpression,
			"timezone":        definition.Timezone,
			"execute_at_ms":   definition.ExecuteAtMS,
			"payload":         []byte(definition.Payload),
			"updated_at_ms":   nowMS,
		})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return tasksvc.ErrNotFound
	}
	return nil
}

// UpdateTaskWorker 覆盖已经由任务服务校验过的 Worker 配置。
func (s *Store) UpdateTaskWorker(ctx context.Context, worker tasksvc.Worker, nowMS int64) error {
	result := s.db.WithContext(ctx).Model(&taskWorkerRow{}).Where("task_definition_id = ?", worker.TaskDefinitionID).
		Updates(map[string]any{
			"handler_type":  worker.HandlerType,
			"concurrency":   worker.Concurrency,
			"max_retry":     worker.MaxRetry,
			"updated_at_ms": nowMS,
		})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return tasksvc.ErrNotFound
	}
	return nil
}

// ListActiveCronTasks 返回需要同步到 Asynq Scheduler 的定时任务。
func (s *Store) ListActiveCronTasks(ctx context.Context) ([]tasksvc.Definition, error) {
	var definitions []taskDefinitionRow
	if err := s.db.WithContext(ctx).
		Where("status = ? AND schedule_type = ?", tasksvc.DefinitionActive, tasksvc.ScheduleCron).
		Order("id ASC").Find(&definitions).Error; err != nil {
		return nil, err
	}
	return s.attachTaskWorkers(ctx, definitions)
}

// ListRunningTaskWorkers 返回当前配置为运行状态的全部 Worker。
func (s *Store) ListRunningTaskWorkers(ctx context.Context) ([]tasksvc.Worker, error) {
	var rows []taskWorkerRow
	if err := s.db.WithContext(ctx).Where("status = ?", tasksvc.WorkerRunning).Order("id ASC").Find(&rows).Error; err != nil {
		return nil, err
	}
	result := make([]tasksvc.Worker, 0, len(rows))
	for _, row := range rows {
		result = append(result, taskWorkerFromRow(row))
	}
	return result, nil
}

// ListFinishedDelayedTaskWorkers 返回已经结束但 Worker 尚未停止的一次性任务。
func (s *Store) ListFinishedDelayedTaskWorkers(ctx context.Context) ([]tasksvc.Worker, error) {
	var rows []taskWorkerRow
	if err := s.db.WithContext(ctx).
		Table("admin_task_workers AS workers").
		Select("workers.*").
		Joins("JOIN admin_task_definitions AS definitions ON definitions.id = workers.task_definition_id").
		Where("workers.status = ? AND definitions.status = ? AND definitions.schedule_type = ?",
			tasksvc.WorkerRunning, tasksvc.DefinitionFinished, tasksvc.ScheduleDelayed).
		Order("workers.id ASC").
		Find(&rows).Error; err != nil {
		return nil, err
	}
	result := make([]tasksvc.Worker, 0, len(rows))
	for _, row := range rows {
		result = append(result, taskWorkerFromRow(row))
	}
	return result, nil
}

// UpdateTaskStatus 更新任务定义状态并返回是否命中记录。
func (s *Store) UpdateTaskStatus(ctx context.Context, id uint64, status string, nowMS int64) error {
	result := s.db.WithContext(ctx).Model(&taskDefinitionRow{}).Where("id = ?", id).
		Updates(map[string]any{"status": status, "updated_at_ms": nowMS})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return tasksvc.ErrNotFound
	}
	return nil
}

// UpdateTaskWorkerStatus 更新任务对应 Worker 的启停状态并返回是否命中记录。
func (s *Store) UpdateTaskWorkerStatus(ctx context.Context, taskID uint64, status string, nowMS int64) error {
	result := s.db.WithContext(ctx).Model(&taskWorkerRow{}).Where("task_definition_id = ?", taskID).
		Updates(map[string]any{"status": status, "updated_at_ms": nowMS})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return tasksvc.ErrNotFound
	}
	return nil
}

// StartTaskExecution 幂等创建执行日志，并在重试时保留首次开始时间。
func (s *Store) StartTaskExecution(ctx context.Context, executionID string, taskID uint64, nowMS int64) error {
	return s.db.WithContext(ctx).Exec(`
INSERT INTO admin_task_executions
  (id, task_definition_id, status, error_message, started_at_ms, finished_at_ms)
VALUES (?, ?, ?, NULL, ?, NULL)
ON DUPLICATE KEY UPDATE
  error_message = IF(status = 'CANCELED', error_message, NULL),
  finished_at_ms = IF(status = 'CANCELED', finished_at_ms, NULL),
  status = IF(status = 'CANCELED', status, VALUES(status))`,
		executionID, taskID, tasksvc.ExecutionRunning, nowMS).Error
}

// FinishTaskExecution 写入执行任务的当前状态、失败原因和可选结束时间。
func (s *Store) FinishTaskExecution(ctx context.Context, executionID, status string, errorMessage *string, finishedAtMS *int64) error {
	query := s.db.WithContext(ctx).Model(&taskExecutionRow{}).Where("id = ?", executionID)
	if status != tasksvc.ExecutionCanceled {
		query = query.Where("status <> ?", tasksvc.ExecutionCanceled)
	}
	return query.
		Updates(map[string]any{
			"status":         status,
			"error_message":  errorMessage,
			"finished_at_ms": finishedAtMS,
		}).Error
}

// ListExecutions 按可选任务主键和状态分页查询全局执行日志。
func (s *Store) ListExecutions(ctx context.Context, taskID *uint64, status string, page, size int) ([]tasksvc.Execution, int64, error) {
	query := s.db.WithContext(ctx).Model(&taskExecutionRow{})
	if taskID != nil {
		query = query.Where("task_definition_id = ?", *taskID)
	}
	if status != "" {
		query = query.Where("status = ?", status)
	}
	var total int64
	if err := query.Count(&total).Error; err != nil {
		return nil, 0, err
	}
	var rows []taskExecutionRow
	if err := query.Order("started_at_ms DESC, id DESC").Limit(size).Offset((page - 1) * size).Find(&rows).Error; err != nil {
		return nil, 0, err
	}
	result := make([]tasksvc.Execution, 0, len(rows))
	for _, row := range rows {
		result = append(result, taskExecutionFromRow(row))
	}
	return result, total, nil
}

// ExecutionByID 按 Execution ID 查询单条执行日志。
func (s *Store) ExecutionByID(ctx context.Context, id string) (*tasksvc.Execution, error) {
	var row taskExecutionRow
	if err := s.db.WithContext(ctx).First(&row, "id = ?", id).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, tasksvc.ErrNotFound
		}
		return nil, err
	}
	result := taskExecutionFromRow(row)
	return &result, nil
}

// attachTaskWorkers 为一组任务定义批量补齐 Worker 配置。
func (s *Store) attachTaskWorkers(ctx context.Context, definitions []taskDefinitionRow) ([]tasksvc.Definition, error) {
	if len(definitions) == 0 {
		return []tasksvc.Definition{}, nil
	}
	ids := make([]uint64, 0, len(definitions))
	for _, definition := range definitions {
		ids = append(ids, definition.ID)
	}
	var workers []taskWorkerRow
	if err := s.db.WithContext(ctx).Where("task_definition_id IN ?", ids).Find(&workers).Error; err != nil {
		return nil, err
	}
	workerByTask := make(map[uint64]taskWorkerRow, len(workers))
	for _, worker := range workers {
		workerByTask[worker.TaskDefinitionID] = worker
	}
	result := make([]tasksvc.Definition, 0, len(definitions))
	for _, definition := range definitions {
		worker, ok := workerByTask[definition.ID]
		if !ok {
			continue
		}
		result = append(result, taskDefinitionFromRows(definition, worker))
	}
	return result, nil
}

// taskDefinitionFromRows 把数据库行转换成任务领域对象。
func taskDefinitionFromRows(definition taskDefinitionRow, worker taskWorkerRow) tasksvc.Definition {
	return tasksvc.Definition{
		ID:             definition.ID,
		Name:           definition.Name,
		Description:    definition.Description,
		ScheduleType:   definition.ScheduleType,
		CronExpression: definition.CronExpression,
		Timezone:       definition.Timezone,
		ExecuteAtMS:    definition.ExecuteAtMS,
		Payload:        append([]byte(nil), definition.Payload...),
		Status:         definition.Status,
		CreatedAtMS:    definition.CreatedAtMS,
		UpdatedAtMS:    definition.UpdatedAtMS,
		Worker:         taskWorkerFromRow(worker),
	}
}

// taskWorkerFromRow 把数据库行转换成 Worker 领域对象。
func taskWorkerFromRow(row taskWorkerRow) tasksvc.Worker {
	return tasksvc.Worker{
		ID:               row.ID,
		TaskDefinitionID: row.TaskDefinitionID,
		HandlerType:      row.HandlerType,
		Concurrency:      row.Concurrency,
		MaxRetry:         row.MaxRetry,
		Status:           row.Status,
		CreatedAtMS:      row.CreatedAtMS,
		UpdatedAtMS:      row.UpdatedAtMS,
	}
}

// taskExecutionFromRow 把数据库行转换成执行日志领域对象。
func taskExecutionFromRow(row taskExecutionRow) tasksvc.Execution {
	return tasksvc.Execution{
		ID:               row.ID,
		TaskDefinitionID: row.TaskDefinitionID,
		Status:           row.Status,
		ErrorMessage:     row.ErrorMessage,
		StartedAtMS:      row.StartedAtMS,
		FinishedAtMS:     row.FinishedAtMS,
	}
}
