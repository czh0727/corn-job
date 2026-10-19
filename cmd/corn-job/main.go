package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"corn-job/handler"
	"corn-job/service/task"
	"corn-job/store/db"
	"corn-job/web"
	"github.com/gin-gonic/gin"
	_ "github.com/go-sql-driver/mysql"
	"github.com/redis/go-redis/v9"
)

type logHandler struct {
	log *slog.Logger
}

func (h logHandler) ValidatePayload(raw json.RawMessage) error {
	if !json.Valid(raw) {
		return fmt.Errorf("payload must be valid JSON")
	}
	return nil
}

func (h logHandler) Execute(ctx context.Context, job task.Job) error {
	h.log.InfoContext(ctx, "task executed", "task_id", job.TaskDefinitionID, "execution_id", job.ExecutionID, "payload", string(job.Payload))
	return nil
}

func main() {
	log := slog.New(slog.NewTextHandler(os.Stdout, nil))
	dsn := os.Getenv("MYSQL_DSN")
	if dsn == "" {
		dsn = "cornjob:cornjob@tcp(127.0.0.1:3310)/cornjob?parseTime=true"
	}
	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "127.0.0.1:6380"
	}
	listenAddr := os.Getenv("LISTEN_ADDR")
	if listenAddr == "" {
		listenAddr = "127.0.0.1:8080"
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	conn, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Error("open mysql", "error", err)
		os.Exit(1)
	}
	defer conn.Close()
	conn.SetMaxOpenConns(16)
	conn.SetMaxIdleConns(4)
	if err := conn.PingContext(ctx); err != nil {
		log.Error("connect mysql", "error", err)
		os.Exit(1)
	}
	store, err := db.New(conn)
	if err != nil {
		log.Error("create task store", "error", err)
		os.Exit(1)
	}
	redisClient := redis.NewClient(&redis.Options{Addr: redisAddr})
	defer redisClient.Close()
	if err := redisClient.Ping(ctx).Err(); err != nil {
		log.Error("connect redis", "error", err)
		os.Exit(1)
	}
	runtime := task.NewRuntime(store, redisClient)
	if err := runtime.Register("log", logHandler{log: log}); err != nil {
		log.Error("register log handler", "error", err)
		os.Exit(1)
	}
	if err := runtime.Start(); err != nil {
		log.Error("start runtime", "error", err)
		os.Exit(1)
	}
	defer runtime.Shutdown()

	engine := gin.New()
	engine.Use(gin.Recovery())
	engine.GET("/", func(c *gin.Context) { c.Redirect(http.StatusFound, "/ui/") })
	engine.GET("/ui/*filepath", gin.WrapH(http.StripPrefix("/ui/", http.FileServer(http.FS(web.Files)))))
	engine.GET("/healthz", func(c *gin.Context) { c.Status(http.StatusNoContent) })
	handler.RegisterRoutes(engine, handler.NewTask(runtime))
	server := &http.Server{Addr: listenAddr, Handler: engine}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()
	log.Info("task API listening", "address", listenAddr)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Error("serve API", "error", err)
	}
}
