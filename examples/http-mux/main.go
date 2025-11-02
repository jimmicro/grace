package main

import (
	"context"
	"fmt"
	"net/http"
	"syscall"
	"time"

	"github.com/jimmicro/grace"
)

// HTTPServer example HTTP server
type HTTPServer struct {
	server *http.Server
}

func NewHTTPServer(addr string) *HTTPServer {
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		time.Sleep(10 * time.Second) // sleep 10 seconds，mock long time processing
		_, _ = w.Write([]byte("Hello, World!"))
	})
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK"))
	})

	return &HTTPServer{
		server: &http.Server{
			Addr:    addr,
			Handler: mux,
		},
	}
}

func (h *HTTPServer) Run(ctx context.Context) error {
	// start server in a separate goroutine
	errCh := make(chan error, 1)
	go func() {
		if err := h.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- err
		}
	}()

	// wait for context done or server error
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
	return fmt.Sprintf("HTTP Server (%s)", h.server.Addr)
}

// BackgroundWorker example background worker service
type BackgroundWorker struct {
	name     string
	interval time.Duration
	stopCh   chan struct{}
}

func NewBackgroundWorker(name string, interval time.Duration) *BackgroundWorker {
	return &BackgroundWorker{
		name:     name,
		interval: interval,
		stopCh:   make(chan struct{}),
	}
}

func (w *BackgroundWorker) Run(ctx context.Context) error {
	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()
	usedTime := time.Now()
	for {
		select {
		case <-ctx.Done():
			return nil
		case t := <-ticker.C:
			fmt.Printf("[%s] execute background task...\n", w.name)
			// if used time is greater than 10 seconds, return error
			if t.Sub(usedTime) > 10*time.Second {
				return fmt.Errorf("used time is greater than 10 seconds")
			}
		case <-w.stopCh:
			return nil
		}
	}
}

func (w *BackgroundWorker) Shutdown(ctx context.Context) error {
	close(w.stopCh)
	fmt.Printf("[%s] background worker stopped\n", w.name)
	return nil
}

func (w *BackgroundWorker) Name() string {
	return w.name
}

func main() {
	// create service instance
	httpServer := NewHTTPServer(":8080")
	worker1 := NewBackgroundWorker("data processor", 5*time.Second)
	worker2 := NewBackgroundWorker("log cleaner", 10*time.Second)

	// create service list
	services := []grace.Grace{
		httpServer,
		worker1,
		worker2,
	}

	// create shepherd
	shepherd := grace.NewShepherd(services,
		grace.WithTimeout(15*time.Second), // 15 seconds timeout
		grace.WithLogger(&customLogger{}), // use custom logger
		grace.WithSignals(syscall.SIGINT,
			syscall.SIGTERM,
			syscall.SIGQUIT),
	)
	shepherd.Start(context.Background())
}

// customLogger example custom logger
type customLogger struct{}

func (l *customLogger) Info(msg string, args ...interface{}) {
	fmt.Printf("[INFO] %s "+msg+"\n", append([]interface{}{time.Now().Format("15:04:05")}, args...)...)
}

func (l *customLogger) Error(msg string, args ...interface{}) {
	fmt.Printf("[ERROR] %s "+msg+"\n", append([]interface{}{time.Now().Format("15:04:05")}, args...)...)
}
