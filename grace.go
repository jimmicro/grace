// Package grace provides a general solution for graceful startup and shutdown of services
package grace

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// Grace defines the interface for graceful startup and shutdown
type Grace interface {
	// Run is the main function to run the service
	Run(ctx context.Context) error
	// Shutdown is the function to shutdown the service
	Shutdown(ctx context.Context) error
	// Name is the name of the service
	Name() string
}

var (
	defaultSignals = []os.Signal{
		syscall.SIGQUIT, // quit
		syscall.SIGTERM, // terminate
		syscall.SIGINT,  // interrupt
		syscall.SIGABRT, // abort
		syscall.SIGSEGV, // segmentation violation
	}
	defaultTimeout = 30 * time.Second
)

// Shepherd manages the graceful startup and shutdown of multiple services
// it will listen for the signals and shutdown the services gracefully
type Shepherd struct {
	services []Grace
	timeout  time.Duration
	signals  []os.Signal
	logger   Logger
}

// Option defines the option for the Shepherd
type Option func(*Shepherd)

// WithTimeout sets the timeout for the shutdown
// if the timeout is reached, the service will be force shutdown
func WithTimeout(timeout time.Duration) Option {
	return func(s *Shepherd) {
		s.timeout = timeout
	}
}

// WithSignals sets the signals to listen for
// if the signal is received, the service will be shutdown gracefully
func WithSignals(signals ...os.Signal) Option {
	return func(s *Shepherd) {
		s.signals = signals
	}
}

// WithLogger sets the logger for the Shepherd
// if the logger is not set, the default logger will be used
func WithLogger(logger Logger) Option {
	return func(s *Shepherd) {
		s.logger = logger
	}
}

// NewShepherd creates a new Shepherd
// it will create a new Shepherd with the given services and options
func NewShepherd(services []Grace, opts ...Option) *Shepherd {
	s := &Shepherd{
		services: services,
	}

	for _, opt := range opts {
		opt(s)
	}

	if s.timeout == 0 {
		s.timeout = defaultTimeout
	}

	if len(s.signals) == 0 {
		s.signals = defaultSignals
	}

	if s.logger == nil {
		s.logger = &defaultLogger{}
	}

	return s
}

func (s *Shepherd) StartErr(ctx context.Context) error {

	if len(s.services) == 0 {
		return fmt.Errorf("no services to start")
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, s.signals...)
	signal.Ignore(syscall.SIGPIPE, syscall.SIGHUP)

	// show startup overview
	s.logger.Info("[STARTUP] starting %d services...", len(s.services))

	var wg sync.WaitGroup
	errCh := make(chan error, len(s.services))

	for i, service := range s.services {
		wg.Add(1)
		go func(idx int, svc Grace) {
			defer wg.Done()
			s.logger.Info("[START] [%d/%d] starting service: %s", idx+1, len(s.services), svc.Name())
			if err := svc.Run(ctx); err != nil {
				s.logger.Error("[START_FAILED] [%d/%d] service %s failed: %v", idx+1, len(s.services), svc.Name(), err)
				errCh <- fmt.Errorf("service %s run error: %w", svc.Name(), err)
			}
		}(i, service)
	}

	select {
	case sig := <-sigCh:
		s.logger.Info("[SIGNAL] received signal [%v], starting graceful shutdown...", sig)
		cancel()
		return s.gracefulShutdown()
	case err := <-errCh:
		s.logger.Error("[SERVICE_ERROR] service error detected, error: [%v], starting graceful shutdown...", err)
		cancel()
		return s.gracefulShutdown()
	}
}

// Start starts all services and listens for shutdown signals
// it will start all services and listen for the signals
// if the signal is received, the services will be shutdown gracefully
func (s *Shepherd) Start(ctx context.Context) {
	err := s.StartErr(ctx)
	if err != nil {
		s.logger.Error("[EXIT] application exit, error: [%v]", err)
		os.Exit(1)
	}
}

// gracefulShutdown executes graceful shutdown
// it will shutdown all services gracefully
// if the timeout is reached, the services will be force shutdown
func (s *Shepherd) gracefulShutdown() error {
	s.logger.Info("[SHUTDOWN] starting graceful shutdown (timeout: [%v])...", s.timeout)

	ctx, cancel := context.WithTimeout(context.Background(), s.timeout)
	defer cancel()

	var wg sync.WaitGroup
	errCh := make(chan error, len(s.services))
	var successCount int64

	for i, service := range s.services {
		wg.Add(1)
		go func(idx int, svc Grace) {
			defer wg.Done()
			s.logger.Info("[STOPPING] [%d/%d] shutting down service: %s", idx+1, len(s.services), svc.Name())
			if err := svc.Shutdown(ctx); err != nil {
				s.logger.Error("[SHUTDOWN_FAILED] [%d/%d] service %s shutdown failed: [%v]", idx+1, len(s.services), svc.Name(), err)
				errCh <- fmt.Errorf("shutdown service %s error: %w", svc.Name(), err)
			} else {
				atomic.AddInt64(&successCount, 1)
				s.logger.Info("[SUCCESS] [%d/%d] service %s shutdown successfully", idx+1, len(s.services), svc.Name())
			}
		}(i, service)
	}

	// wait for all services to shutdown or timeout
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		finalCount := atomic.LoadInt64(&successCount)
		s.logger.Info("[COMPLETED] all services shutdown completed ([%d/%d] successful)", finalCount, len(s.services))
		return nil
	case <-ctx.Done():
		finalCount := atomic.LoadInt64(&successCount)
		s.logger.Error("[TIMEOUT] shutdown timeout reached! force exit ([%d/%d] completed)", finalCount, len(s.services))
		return fmt.Errorf("shutdown timeout")
	case err := <-errCh:
		go func() {
			<-done // wait for other services to shutdown
		}()
		return err
	}
}
