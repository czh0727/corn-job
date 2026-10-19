package task

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/hibiken/asynq"
)

const (
	runtimeLeaderKey    = "admin:taskruntime:leader" //避免多pod Scheduler重复发放job
	runtimeEpochKey     = "admin:taskruntime:epoch"  //记录是否为当前leader发的任务
	leaderLeaseTTL      = 30 * time.Second
	leaderRenewInterval = 10 * time.Second
	leaderRetryInterval = 3 * time.Second
)

var renewLeaderScript = `
if redis.call("GET", KEYS[1]) == ARGV[1] then
  return redis.call("PEXPIRE", KEYS[1], ARGV[2])
end
return 0`

var releaseLeaderScript = `
if redis.call("GET", KEYS[1]) == ARGV[1] then
  return redis.call("DEL", KEYS[1])
end
return 0`

type periodicConfigs struct {
	store taskStore
	epoch int64
}

// GetConfigs 从数据库加载已启用 Cron 任务，供 Asynq 周期性同步调度项。PeriodicTaskManager 启动后会执行
// 实现PeriodicTaskConfigProvider
func (p *periodicConfigs) GetConfigs() ([]*asynq.PeriodicTaskConfig, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// 查出在执行的任务
	definitions, err := p.store.ListActiveCronTasks(ctx)
	if err != nil {
		return nil, err
	}
	configs := make([]*asynq.PeriodicTaskConfig, 0, len(definitions))
	for _, definition := range definitions {
		payload, err := json.Marshal(triggerEnvelope{TaskDefinitionID: definition.ID, RuntimeEpoch: p.epoch})
		if err != nil {
			return nil, fmt.Errorf("marshal cron trigger: %w", err)
		}
		configs = append(configs, &asynq.PeriodicTaskConfig{
			Cronspec: cronSpec(definition),
			Task:     asynq.NewTask(asynqDispatchType, payload),
			Opts: []asynq.Option{
				asynq.Queue(dispatchQueue),
				asynq.MaxRetry(MaxRetryCount),
			},
		})
	}
	return configs, nil
}

type runningWorker struct {
	config Worker
	server *asynq.Server
}

// leadershipLoop 持续竞争 Redis 租约，保证多实例中仅 Leader 启动调度与消费。
func (r *Runtime) leadershipLoop(ctx context.Context) {
	owner := uuid.NewString()
	for ctx.Err() == nil {
		acquired, err := r.redis.SetNX(ctx, runtimeLeaderKey, owner, leaderLeaseTTL).Result()
		if err != nil {
			r.log.ErrorContext(ctx, "task runtime leader acquire failed", "error", err)
			if !waitContext(ctx, leaderRetryInterval) {
				return
			}
			continue
		}
		if !acquired {
			if !waitContext(ctx, leaderRetryInterval) {
				return
			}
			continue
		}

		r.log.InfoContext(ctx, "task runtime leader acquired", "owner", owner)
		r.runLeadershipSession(ctx, owner)
		if ctx.Err() == nil {
			_ = waitContext(ctx, leaderRetryInterval)
		}
	}
}

// runLeadershipSession 在持有租约期间运行 Scheduler、Worker Reconciler 和续租循环。
func (r *Runtime) runLeadershipSession(parent context.Context, owner string) {
	ctx, cancel := context.WithCancel(parent)
	renewDone := make(chan struct{})
	go func() {
		defer close(renewDone)
		// 续租
		ticker := time.NewTicker(leaderRenewInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				ok, err := r.renewLeadership(ctx, owner)
				if err != nil || !ok {
					r.log.WarnContext(ctx, "task runtime leader lease lost", "error", err)
					cancel()
					return
				}
			}
		}
	}()

	if err := r.runLeader(ctx); err != nil && ctx.Err() == nil {
		r.log.ErrorContext(ctx, "task runtime leader stopped", "error", err)
	}
	cancel()
	<-renewDone
	if err := r.releaseLeadership(context.Background(), owner); err != nil {
		r.log.Warn("task runtime leader release failed", "error", err)
	}
}

// runLeader 启动数据库驱动的 Asynq Scheduler，并动态对齐全部运行中 Worker。
func (r *Runtime) runLeader(ctx context.Context) error {
	epoch, err := r.redis.Incr(ctx, runtimeEpochKey).Result()
	if err != nil {
		return fmt.Errorf("increment task runtime epoch: %w", err)
	}
	dispatchServer := asynq.NewServerFromRedisClient(r.redis, asynq.Config{
		Concurrency:     10,
		Queues:          map[string]int{dispatchQueue: 1},
		ShutdownTimeout: 10 * time.Second,
	})
	if err := dispatchServer.Start(asynq.HandlerFunc(r.processDispatch)); err != nil {
		return fmt.Errorf("start task dispatch server: %w", err)
	}
	//  Scheduler 到点后把任务放到 admin_task_dispatch 与延时任务不同可能任务会暂停
	manager, err := asynq.NewPeriodicTaskManager(asynq.PeriodicTaskManagerOpts{
		PeriodicTaskConfigProvider: &periodicConfigs{store: r.store, epoch: epoch},
		RedisConnOpt:               r.schedulerRedis,
		SyncInterval:               workerSyncInterval, //每workerSyncInterval更新任务状态 可能会出现5s状态不一样，需要dispatchServer控制
		SchedulerOpts: &asynq.SchedulerOpts{
			Location: time.UTC,
		},
	})
	if err != nil {
		dispatchServer.Stop()
		dispatchServer.Shutdown()
		return fmt.Errorf("create periodic task manager: %w", err)
	}
	if err := manager.Start(); err != nil {
		dispatchServer.Stop()
		dispatchServer.Shutdown()
		return fmt.Errorf("start periodic task manager: %w", err)
	}
	r.log.InfoContext(ctx, "task scheduler started", "runtime_epoch", epoch)

	workers := make(map[uint64]*runningWorker)
	// 读取状态，并且更新worker
	if err := r.reconcileWorkers(ctx, workers); err != nil {
		r.log.ErrorContext(ctx, "initial task worker reconcile failed", "error", err)
	}
	// 定期更新worker状态（起停）
	ticker := time.NewTicker(workerSyncInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			manager.Shutdown()
			dispatchServer.Stop()
			dispatchServer.Shutdown()
			r.stopAllWorkers(workers)
			r.log.Info("task scheduler and workers stopped")
			return nil
		case <-ticker.C:
			if err := r.reconcileWorkers(ctx, workers); err != nil {
				r.log.ErrorContext(ctx, "task worker reconcile failed", "error", err)
			}
		case <-r.reconcileCh:
			if err := r.reconcileWorkers(ctx, workers); err != nil {
				r.log.ErrorContext(ctx, "task worker reconcile failed", "error", err)
			}
		}
	}
}

// reconcileWorkers 按数据库 Worker 状态创建、重启或关闭任务专属 Asynq Server。
func (r *Runtime) reconcileWorkers(ctx context.Context, running map[uint64]*runningWorker) error {
	desired, err := r.store.ListRunningTaskWorkers(ctx)
	if err != nil {
		return err
	}
	desiredByTask := make(map[uint64]Worker, len(desired))
	for _, worker := range desired {
		desiredByTask[worker.TaskDefinitionID] = worker
	}
	// 回收延时任务worker
	finishedDelayed, err := r.store.ListFinishedDelayedTaskWorkers(ctx)
	if err != nil {
		return err
	}
	cleanupQueues := make(map[uint64]struct{}, len(finishedDelayed))
	//一次性延时任务执行完成后资源回收
	if len(finishedDelayed) > 0 {
		queueNames, err := r.inspector.Queues()
		if err != nil {
			return fmt.Errorf("list queues before finished delayed cleanup: %w", err)
		}
		existingQueues := make(map[string]struct{}, len(queueNames))
		for _, queue := range queueNames {
			existingQueues[queue] = struct{}{}
		}
		for _, worker := range finishedDelayed {
			queue := queueName(worker.TaskDefinitionID)
			if _, exists := existingQueues[queue]; exists {
				info, err := r.inspector.GetQueueInfo(queue)
				if err != nil {
					return fmt.Errorf("inspect finished delayed queue for task %d: %w", worker.TaskDefinitionID, err)
				}
				if info.Pending+info.Active+info.Scheduled+info.Retry+info.Aggregating != 0 {
					continue
				}
			}
			delete(desiredByTask, worker.TaskDefinitionID)
			cleanupQueues[worker.TaskDefinitionID] = struct{}{}
		}
	}

	for taskID, current := range running {
		worker, exists := desiredByTask[taskID]
		if exists && sameWorkerConfig(current.config, worker) {
			if err := r.unpauseQueueIfNeeded(queueName(taskID)); err != nil {
				return fmt.Errorf("unpause queue for task %d: %w", taskID, err)
			}
			continue
		}
		stopRunningWorker(current)
		delete(running, taskID)
	}

	for taskID := range cleanupQueues {
		if err := r.inspector.DeleteQueue(queueName(taskID), true); err != nil && !errors.Is(err, asynq.ErrQueueNotFound) {
			return fmt.Errorf("delete finished delayed queue for task %d: %w", taskID, err)
		}
		if err := r.store.UpdateTaskWorkerStatus(ctx, taskID, WorkerStopped, time.Now().UnixMilli()); err != nil {
			return fmt.Errorf("stop finished delayed worker for task %d: %w", taskID, err)
		}
		r.log.InfoContext(ctx, "finished delayed worker stopped", "task_id", taskID)
	}
	for taskID, worker := range desiredByTask {
		if _, exists := running[taskID]; exists {
			continue
		}
		server := asynq.NewServerFromRedisClient(r.redis, asynq.Config{
			Concurrency:     worker.Concurrency,
			Queues:          map[string]int{queueName(taskID): 1},
			ShutdownTimeout: 10 * time.Second,
		})
		if err := server.Start(asynq.HandlerFunc(r.processTask)); err != nil {
			return fmt.Errorf("start worker for task %d: %w", taskID, err)
		}
		if err := r.unpauseQueueIfNeeded(queueName(taskID)); err != nil {
			server.Stop()
			server.Shutdown()
			return fmt.Errorf("unpause queue for task %d: %w", taskID, err)
		}
		running[taskID] = &runningWorker{config: worker, server: server}
		r.log.InfoContext(ctx, "task worker started", "task_id", taskID, "concurrency", worker.Concurrency)
	}
	return nil
}

// stopAllWorkers 优雅停止当前实例持有的全部 Worker Server。
func (r *Runtime) stopAllWorkers(workers map[uint64]*runningWorker) {
	for taskID, worker := range workers {
		stopRunningWorker(worker)
		delete(workers, taskID)
	}
}

// stopRunningWorker 先停止拉取新任务，再等待在途任务结束或回到 Redis。
func stopRunningWorker(worker *runningWorker) {
	if worker == nil || worker.server == nil {
		return
	}
	worker.server.Stop()
	worker.server.Shutdown()
}

// sameWorkerConfig 判断运行中的 Server 是否需要按新配置重建。
func sameWorkerConfig(left, right Worker) bool {
	return left.HandlerType == right.HandlerType && left.Concurrency == right.Concurrency
}

// renewLeadership 仅在租约仍属于本实例时延长 Redis 过期时间。
func (r *Runtime) renewLeadership(ctx context.Context, owner string) (bool, error) {
	result, err := r.redis.Eval(ctx, renewLeaderScript, []string{runtimeLeaderKey}, owner, leaderLeaseTTL.Milliseconds()).Int64()
	return result == 1, err
}

// releaseLeadership 仅删除本实例仍然持有的 Redis Leader 租约。
func (r *Runtime) releaseLeadership(ctx context.Context, owner string) error {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	return r.redis.Eval(ctx, releaseLeaderScript, []string{runtimeLeaderKey}, owner).Err()
}

// waitContext 等待指定时长，并在运行时退出时提前返回。
func waitContext(ctx context.Context, duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
