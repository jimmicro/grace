package grace

import (
	"context"
	"sync"
)

// WaitWithTimeout waits for the WaitGroup to complete or context to timeout
// Returns nil if all goroutines complete successfully
// Returns context.Cause(ctx) if timeout occurs
func WaitWithTimeout(wg *sync.WaitGroup, ctx context.Context) error {
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}

// ServiceConfig holds the configuration for service execution
type ServiceConfig struct {
	serviceName      string
	graceWg          *GraceWaitGroup
	logger           Logger
	stopOnFirstError bool // whether to stop all tasks when one task fails
}

// ServiceOption defines the option for service
type ServiceOption func(*ServiceConfig)

// WithServiceLogger sets a custom logger for the service
func WithServiceLogger(logger Logger) ServiceOption {
	return func(c *ServiceConfig) {
		c.logger = logger
	}
}

// WithStopOnFirstError sets whether to stop all tasks when one task fails
func WithStopOnFirstError(stopOnFirstError bool) ServiceOption {
	return func(c *ServiceConfig) {
		c.stopOnFirstError = stopOnFirstError
	}
}

// RunService runs multiple tasks concurrently and manages their lifecycle
// serviceName: service name for logging
// graceWg: WaitGroup for graceful shutdown tracking, can be nil
// tasks: task functions to execute
// opts: optional configurations
//
// The service will:
// - Start all tasks concurrently
// - Wait for context cancellation or task error
// - Wait for all tasks to complete before returning
func RunService(
	ctx context.Context,
	serviceName string,
	graceWg *GraceWaitGroup,
	tasks []func(context.Context) error,
	opts ...ServiceOption,
) error {
	config := &ServiceConfig{
		serviceName:      serviceName,
		graceWg:          graceWg,
		logger:           &defaultLogger{},
		stopOnFirstError: true,
	}

	for _, opt := range opts {
		opt(config)
	}

	config.logger.Info("[%s] starting all background tasks...", serviceName)

	errChan := make(chan error, len(tasks))

	// Start all tasks
	for _, task := range tasks {
		taskFunc := task // avoid closure issues
		if graceWg != nil {
			graceWg.Go(func() {
				if err := taskFunc(ctx); err != nil {
					select {
					case errChan <- err:
					default:
					}
				}
			})
		} else {
			go func() {
				if err := taskFunc(ctx); err != nil {
					select {
					case errChan <- err:
					default:
					}
				}
			}()
		}
	}

	config.logger.Info("[%s] all background tasks started", serviceName)

	// Wait for error or context cancellation
	select {
	case err := <-errChan:
		// A task encountered an error
		config.logger.Error("[%s] task error: %v", serviceName, err)
		if config.stopOnFirstError {
			config.logger.Info("[%s] waiting for running tasks to complete...", serviceName)
			if graceWg != nil {
				graceWg.Wait()
			}
			config.logger.Info("[%s] all tasks completed", serviceName)
			return err
		}
		// Continue running other tasks
		<-ctx.Done()
	case <-ctx.Done():
		// Context was cancelled
		config.logger.Info("[%s] received shutdown signal", serviceName)
	}

	config.logger.Info("[%s] waiting for running tasks to complete...", serviceName)
	if graceWg != nil {
		graceWg.Wait()
	}
	config.logger.Info("[%s] all tasks completed", serviceName)

	return context.Cause(ctx)
}
