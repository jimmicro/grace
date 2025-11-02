package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jimmicro/grace"
)

const (
	// ShutdownTimeout is the timeout for graceful shutdown
	ShutdownTimeout = 100 * time.Second
)

func main() {
	log.Default().Println("========================================")
	log.Default().Println("[Server] Starting server...")
	log.Default().Println("========================================")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM, syscall.SIGINT)

	server := NewServer()
	runErr := make(chan error, 1)
	go func() {
		err := server.Run(ctx)
		if err != nil {
			log.Default().Println("[Server] [ERROR] Run error:", err)
		}
		runErr <- err
	}()

	select {
	case err := <-runErr:
		// Service encountered an error
		log.Default().Println("\n========================================")
		log.Default().Println("[Server] [ERROR] Run error:", err)
		log.Default().Println("========================================")
		cancel()
	case s := <-quit:
		// Received shutdown signal
		log.Default().Println("\n========================================")
		log.Default().Println("[Server] [SIGNAL] Received signal:", s)
		log.Default().Println("[Server] [SHUTDOWN] Initiating graceful shutdown...")
		log.Default().Println("========================================")
		cancel()
	}

	// Wait for graceful shutdown with timeout
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), ShutdownTimeout)
	defer shutdownCancel()

	if err := server.Close(shutdownCtx); err != nil {
		log.Default().Println("\n========================================")
		log.Default().Println("[Server] [ERROR] Close error:", err)
		log.Default().Println("========================================")
	} else {
		log.Default().Println("\n========================================")
		log.Default().Println("[Server] [SUCCESS] Server closed gracefully")
		log.Default().Println("========================================")
	}
}

// Server is the main server that manages multiple services
type Server struct {
	SeeHelloService  *SeeHelloService
	CalculateService *CalculateService
	ExecuteService   *ExecuteService
	graceWg          grace.GraceWaitGroup
}

func NewServer() *Server {
	s := &Server{}
	s.SeeHelloService = NewSeeHelloService()
	s.CalculateService = NewCalculateService()
	s.ExecuteService = NewExecuteService()
	return s
}

func (s *Server) Run(ctx context.Context) error {
	tasks := []func(context.Context) error{
		s.SeeHelloService.Run,
		s.CalculateService.Run,
		s.ExecuteService.Run,
	}
	return grace.RunService(ctx, "Server", &s.graceWg, tasks)
}

func (s *Server) Close(ctx context.Context) error {
	log.Default().Println("[Server] Waiting for all services to stop...")

	// Use the utility function for timeout waiting
	if err := grace.WaitWithTimeout(&s.graceWg.WaitGroup, ctx); err != nil {
		log.Default().Println("[Server] [TIMEOUT] Shutdown timeout reached")
		return err
	}

	log.Default().Println("[Server] [SUCCESS] All services stopped")
	return nil
}

// SeeHelloService is a service that prints hello periodically
type SeeHelloService struct {
	graceWg grace.GraceWaitGroup
}

func NewSeeHelloService() *SeeHelloService {
	return &SeeHelloService{}
}

func (s *SeeHelloService) Run(ctx context.Context) error {
	tasks := []func(context.Context) error{
		s.runSayHelloTask,
	}
	return grace.RunService(ctx, "SeeHelloService", &s.graceWg, tasks)
}

// runSayHelloTask executes sayHello every 3 seconds
func (s *SeeHelloService) runSayHelloTask(ctx context.Context) error {
	return grace.RunPeriodicTask(
		ctx,
		"SayHelloTask",
		3*time.Second,
		s.sayHello,
		grace.WithRunOnStart(false),
		grace.WithStopOnTaskError(true),
	)
}

func (s *SeeHelloService) sayHello(ctx context.Context, t time.Time) error {
	log.Default().Println("[SayHelloTask] begin", t.Format(time.DateTime))
	log.Default().Println("[SayHelloTask] saying hello, mock cost 3 seconds")
	time.Sleep(3 * time.Second) // mock greeting task
	log.Default().Println("[SayHelloTask] Hello, World!")
	log.Default().Println("[SayHelloTask] end", t.Format(time.DateTime))
	return nil
}

// CalculateService is a service that performs calculations periodically
type CalculateService struct {
	graceWg grace.GraceWaitGroup
}

func NewCalculateService() *CalculateService {
	return &CalculateService{}
}

func (s *CalculateService) Run(ctx context.Context) error {
	tasks := []func(context.Context) error{
		s.runCalculateTask,
	}
	return grace.RunService(ctx, "CalculateService", &s.graceWg, tasks)
}

// runCalculateTask executes calculate every 2 seconds
func (s *CalculateService) runCalculateTask(ctx context.Context) error {
	return grace.RunPeriodicTask(
		ctx,
		"CalculateTask",
		2*time.Second,
		s.calculate,
	)
}

// calculate performs 1 + 1 calculation
func (s *CalculateService) calculate(ctx context.Context, t time.Time) error {
	log.Default().Println("[CalculateTask] begin", t.Format(time.DateTime))
	log.Default().Println("[CalculateTask] calculating 1 + 1, mock cost 5 seconds")
	time.Sleep(5 * time.Second) // mock calculation task
	result := 1 + 1
	log.Default().Println("[CalculateTask] result:", result)
	log.Default().Println("[CalculateTask] end", t.Format(time.DateTime))
	return nil
}

// ExecuteService is a service that executes tasks periodically
type ExecuteService struct {
	graceWg grace.GraceWaitGroup
}

func NewExecuteService() *ExecuteService {
	return &ExecuteService{}
}

func (s *ExecuteService) Run(ctx context.Context) error {
	tasks := []func(context.Context) error{
		s.runExecuteTask,
	}
	return grace.RunService(ctx, "ExecuteService", &s.graceWg, tasks)
}

// runExecuteTask executes execute every second
func (s *ExecuteService) runExecuteTask(ctx context.Context) error {
	return grace.RunPeriodicTask(
		ctx,
		"ExecuteTask",
		1*time.Second,
		s.execute,
	)
}

func (s *ExecuteService) execute(ctx context.Context, t time.Time) error {
	log.Default().Println("[ExecuteTask] begin", t.Format(time.DateTime))
	log.Default().Println("[ExecuteTask] mock cost 10 seconds")
	time.Sleep(10 * time.Second) // mock time-consuming operation
	log.Default().Println("[ExecuteTask] end", t.Format(time.DateTime))
	return nil
}

// ErrorService demonstrates error handling
type ErrorService struct {
	graceWg grace.GraceWaitGroup
}

func NewErrorService() *ErrorService {
	return &ErrorService{}
}

func (s *ErrorService) Run(ctx context.Context) error {
	log.Default().Println("[ErrorService] Starting all background tasks...")
	return fmt.Errorf("execute task error")
}
