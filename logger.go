package grace

import (
	"log"
)

// Logger defines the interface for logging in grace components
// Implementations can provide custom logging behavior
type Logger interface {
	// Info logs an informational message with optional arguments
	Info(msg string, args ...interface{})
	// Error logs an error message with optional arguments
	Error(msg string, args ...interface{})
}

// defaultLogger is the default logger implementation using standard log package
type defaultLogger struct{}

// Info logs an informational message to standard output
func (l *defaultLogger) Info(msg string, args ...interface{}) {
	log.Default().Printf("[INFO] "+msg+"\n", args...)
}

// Error logs an error message to standard output
func (l *defaultLogger) Error(msg string, args ...interface{}) {
	log.Default().Printf("[ERROR] "+msg+"\n", args...)
}
