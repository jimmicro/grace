# Grace - Graceful Service Management Library

Grace is a comprehensive Go library for graceful startup and shutdown of services. It provides both low-level utilities for building services and high-level orchestration for managing multiple services.

[![Check](https://github.com/jimmicro/grace/actions/workflows/check.yaml/badge.svg)](https://github.com/jimmicro/grace/actions/workflows/check.yaml)

## Features

### Core Components

1. **Shepherd** - High-level service orchestrator
   - Manages multiple services with unified lifecycle
   - Signal handling (SIGINT, SIGTERM, etc.)
   - Configurable shutdown timeout
   - Custom logger support

2. **Utility Functions** - Low-level building blocks
   - `GraceWaitGroup` - Enhanced WaitGroup with `Go()` method
   - `WaitWithTimeout` - Wait for WaitGroup with timeout
   - `RunPeriodicTask` - Execute tasks on a schedule
   - `RunService` - Manage multiple concurrent tasks

## Installation

```bash
go get github.com/jimmicro/grace
```

## Quick Start

### Using Shepherd (High-level API)

Perfect for managing multiple independent services:

```go
package main

import (
    "context"
    "time"
    "github.com/jimmicro/grace"
)

// Implement the Grace interface for your service
type MyService struct{}

func (s *MyService) Run(ctx context.Context) error {
    // Your service logic here
    <-ctx.Done()
    return nil
}

func (s *MyService) Shutdown(ctx context.Context) error {
    // Cleanup logic here
    return nil
}

func (s *MyService) Name() string {
    return "MyService"
}

func main() {
    services := []grace.Grace{
        &MyService{},
    }

    shepherd := grace.NewShepherd(services,
        grace.WithTimeout(30*time.Second),
        grace.WithLogger(&customLogger{}),
    )

    shepherd.Start(context.Background())
}
```

### Using Utility Functions (Low-level API)

Perfect for building custom services with fine-grained control:

```go
package main

import (
    "context"
    "log"
    "time"
    "github.com/jimmicro/grace"
)

func main() {
    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()

    var wg grace.GraceWaitGroup

    // Start a periodic task
    wg.Go(func() {
        grace.RunPeriodicTask(
            ctx,
            "MyTask",
            5*time.Second,
            func(ctx context.Context, t time.Time) error {
                log.Println("Task executed at", t)
                return nil
            },
            grace.WithRunOnStart(true),
            grace.WithStopOnTaskError(false),
        )
    })

    // Wait for completion
    wg.Wait()
}
```

## API Reference

### Shepherd

#### Creating a Shepherd

```go
shepherd := grace.NewShepherd(services, options...)
```

**Options:**
- `WithTimeout(duration)` - Set shutdown timeout (default: 30s)
- `WithSignals(signals...)` - Set signals to listen for
- `WithLogger(logger)` - Set custom logger

#### Grace Interface

Services managed by Shepherd must implement:

```go
type Grace interface {
    Run(ctx context.Context) error      // Start the service
    Shutdown(ctx context.Context) error // Graceful shutdown
    Name() string                       // Service name for logging
}
```

### Utility Functions

#### GraceWaitGroup

Enhanced `sync.WaitGroup` with automatic management:

```go
var wg grace.GraceWaitGroup

// Automatically calls Add(1) and Done()
wg.Go(func() {
    // Your goroutine code
})

wg.Wait()
```

#### WaitWithTimeout

Wait for WaitGroup with context timeout:

```go
ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
defer cancel()

err := grace.WaitWithTimeout(&wg.WaitGroup, ctx)
if err != nil {
    // Timeout occurred
}
```

#### RunPeriodicTask

Execute a task periodically:

```go
err := grace.RunPeriodicTask(
    ctx,
    "TaskName",
    interval,
    taskFunc,
    grace.WithRunOnStart(true),        // Run immediately on start
    grace.WithStopOnTaskError(false),  // Continue on error
    grace.WithTaskLogger(logger),      // Custom logger
)
```

**Task Function Signature:**
```go
func(ctx context.Context, t time.Time) error
```

#### RunService

Manage multiple concurrent tasks:

```go
tasks := []func(context.Context) error{
    task1,
    task2,
    task3,
}

err := grace.RunService(
    ctx,
    "ServiceName",
    &graceWg,
    tasks,
    grace.WithStopOnFirstError(true),  // Stop all on first error
    grace.WithServiceLogger(logger),   // Custom logger
)
```

## Examples

### Example 1: HTTP Server with Shepherd

```go
type HTTPServer struct {
    server *http.Server
}

func (h *HTTPServer) Run(ctx context.Context) error {
    errCh := make(chan error, 1)
    go func() {
        if err := h.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
            errCh <- err
        }
    }()

    select {
    case <-ctx.Done():
        return nil
    case err := <-errCh:
        return err
    }
}

func (h *HTTPServer) Shutdown(ctx context.Context) error {
    return h.server.Shutdown(ctx)
}

func (h *HTTPServer) Name() string {
    return "HTTPServer"
}
```

See [examples/http-mux/main.go](examples/http-mux/main.go) for complete example.

### Example 2: Periodic Task Service

```go
type PeriodicService struct {
    wg grace.GraceWaitGroup
}

func (s *PeriodicService) Run(ctx context.Context) error {
    return grace.RunPeriodicTask(
        ctx,
        "PeriodicTask",
        5*time.Second,
        func(ctx context.Context, t time.Time) error {
            // Your task logic
            return nil
        },
    )
}
```

See [examples/utils-demo/main.go](examples/utils-demo/main.go) for complete example.

### Example 3: Multi-Task Service

```go
type MultiTaskService struct {
    wg grace.GraceWaitGroup
}

func (s *MultiTaskService) Run(ctx context.Context) error {
    tasks := []func(context.Context) error{
        s.task1,
        s.task2,
        s.task3,
    }

    return grace.RunService(ctx, "MultiTaskService", &s.wg, tasks)
}

func (s *MultiTaskService) task1(ctx context.Context) error {
    return grace.RunPeriodicTask(ctx, "Task1", 3*time.Second, s.doTask1)
}
```

See [examples/single-service/demo_service.go](examples/single-service/demo_service.go) for complete example.

## Architecture

```
grace/
├── Shepherd (High-level orchestration)
│   ├── Service lifecycle management
│   ├── Signal handling
│   └── Timeout control
│
└── Utilities (Low-level building blocks)
    ├── GraceWaitGroup - Enhanced WaitGroup
    ├── WaitWithTimeout - Timeout waiting
    ├── RunPeriodicTask - Periodic execution
    └── RunService - Task management
```

## Design Principles

1. **Composability** - Utilities can be used independently or together
2. **Flexibility** - Both high-level and low-level APIs available
3. **Safety** - Proper context handling and timeout control
4. **Observability** - Configurable logging throughout
5. **Simplicity** - Clean, idiomatic Go code

## Best Practices

1. **Always use context for cancellation**
   ```go
   // Good
   func task(ctx context.Context) error {
       select {
       case <-ctx.Done():
           return ctx.Err()
       case <-time.After(time.Second):
           // Do work
       }
   }
   ```

2. **Set appropriate timeouts**
   ```go
   shepherd := grace.NewShepherd(services,
       grace.WithTimeout(30*time.Second), // Allow enough time for cleanup
   )
   ```

3. **Handle errors properly**
   ```go
   err := grace.RunPeriodicTask(ctx, "Task", interval, taskFunc,
       grace.WithStopOnTaskError(false), // Continue on non-critical errors
   )
   ```

4. **Use GraceWaitGroup for automatic management**
   ```go
   var wg grace.GraceWaitGroup
   wg.Go(func() { /* work */ }) // Cleaner than wg.Add(1) + defer wg.Done()
   ```

## Development

```bash
golangci-lint run ./...
go test ./...
```

## Contributing

Contributions are welcome! Please ensure:
- All code has English comments
- Tests are included for new features
- Examples demonstrate new functionality
- Code follows Go best practices

## License

MIT License - see LICENSE file for details
