package grace

import (
	"context"
	"time"
)

// PeriodicTaskConfig holds the configuration for periodic task execution
type PeriodicTaskConfig struct {
	taskName    string
	interval    time.Duration
	logger      Logger
	runOnStart  bool // whether to run the task immediately on start
	stopOnError bool // whether to stop on task error
}

// PeriodicTaskOption defines the option for periodic task
type PeriodicTaskOption func(*PeriodicTaskConfig)

// WithTaskLogger sets a custom logger for the periodic task
func WithTaskLogger(logger Logger) PeriodicTaskOption {
	return func(c *PeriodicTaskConfig) {
		c.logger = logger
	}
}

// WithRunOnStart sets whether to run the task immediately on start
func WithRunOnStart(runOnStart bool) PeriodicTaskOption {
	return func(c *PeriodicTaskConfig) {
		c.runOnStart = runOnStart
	}
}

// WithStopOnTaskError sets whether to stop the task on error
func WithStopOnTaskError(stopOnError bool) PeriodicTaskOption {
	return func(c *PeriodicTaskConfig) {
		c.stopOnError = stopOnError
	}
}

// RunPeriodicTask runs a task periodically with the given interval
// taskName: task name for logging
// interval: execution interval
// taskFunc: the task function to execute
// opts: optional configurations
//
// The task will stop when:
// - Context is cancelled
// - Task returns an error (if stopOnError is true)
func RunPeriodicTask(
	ctx context.Context,
	taskName string,
	interval time.Duration,
	taskFunc func(context.Context, time.Time) error,
	opts ...PeriodicTaskOption,
) error {
	config := &PeriodicTaskConfig{
		taskName:    taskName,
		interval:    interval,
		logger:      &defaultLogger{},
		runOnStart:  false,
		stopOnError: true,
	}

	for _, opt := range opts {
		opt(config)
	}

	// Run task immediately if configured
	if config.runOnStart {
		select {
		case <-ctx.Done():
			config.logger.Info("[%s] task stopped before first run", taskName)
			return context.Cause(ctx)
		default:
			if err := taskFunc(ctx, time.Now()); err != nil {
				config.logger.Error("[%s] task error on first run: %v", taskName, err)
				if config.stopOnError {
					return err
				}
			}
		}
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			config.logger.Info("[%s] task stopped", taskName)
			return context.Cause(ctx)
		case t := <-ticker.C:
			// Double-check context before executing task
			// This avoids processing stale ticker events when context is cancelled
			select {
			case <-ctx.Done():
				config.logger.Info("[%s] task stopped (skipped pending task)", taskName)
				return context.Cause(ctx)
			default:
				// Context is not cancelled, execute the task
				err := taskFunc(ctx, t)
				if err != nil {
					config.logger.Error("[%s] task error: %v", taskName, err)
					if config.stopOnError {
						return err
					}
				}
			}
		}
	}
}
