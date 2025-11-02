package grace

import "sync"

// GraceWaitGroup is an enhanced sync.WaitGroup with convenient methods
type GraceWaitGroup struct {
	sync.WaitGroup
}

// Go starts a goroutine with automatic WaitGroup management
// It automatically calls Add(1) before starting the goroutine
// and Done() when the goroutine completes
func (g *GraceWaitGroup) Go(f func()) {
	g.Add(1)
	go func() {
		defer g.Done()
		f()
	}()
}
