package handler

import "github.com/gin-gonic/gin"

func RegisterRoutes(engine *gin.Engine, task *Task) {
	api := engine.Group("/api/v1")
	definitions := api.Group("/task-definitions")
	definitions.POST("", task.Create)
	definitions.GET("", task.List)
	definitions.GET("/:id", task.Get)
	definitions.PATCH("/:id", task.Update)
	definitions.POST("/:id/publish", task.Start)
	definitions.POST("/:id/pause", task.Pause)
	definitions.POST("/:id/resume", task.Resume)
	definitions.POST("/:id/run", task.Run)
	definitions.GET("/:id/worker", task.GetWorker)
	definitions.PATCH("/:id/worker", task.UpdateWorker)
	definitions.POST("/:id/worker/start", task.StartWorker)
	definitions.POST("/:id/worker/stop", task.StopWorker)

	executions := api.Group("/task-executions")
	executions.GET("", task.ListExecutions)
	executions.GET("/:id", task.GetExecution)
	executions.POST("/:id/cancel", task.CancelExecution)
}
