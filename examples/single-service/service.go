package main

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"
)

// TaskProcessor 异步任务处理服务
type TaskProcessor struct {
	name       string
	taskQueue  chan Task
	workerPool int
	wg         sync.WaitGroup
	running    bool
	mu         sync.RWMutex
}

// Task 表示一个任务
type Task struct {
	ID       int
	Data     string
	Priority int
	Created  time.Time
}

// NewTaskProcessor 创建新的任务处理器
func NewTaskProcessor(name string, workerPool int, queueSize int) *TaskProcessor {
	return &TaskProcessor{
		name:       name,
		taskQueue:  make(chan Task, queueSize),
		workerPool: workerPool,
	}
}

// Start 启动任务处理服务 (内部组件，不实现 Grace 接口)
func (tp *TaskProcessor) Start(ctx context.Context) error {
	tp.mu.Lock()
	tp.running = true
	tp.mu.Unlock()

	fmt.Printf("[%s] Starting task processor with %d workers\n", tp.name, tp.workerPool)

	// 启动工作协程池
	for i := 0; i < tp.workerPool; i++ {
		tp.wg.Add(1)
		go tp.worker(ctx, i)
	}

	// 启动任务生成器（模拟外部任务）
	tp.wg.Add(1)
	go tp.taskGenerator(ctx)

	// 等待所有工作协程完成
	tp.wg.Wait()
	fmt.Printf("[%s] All workers stopped\n", tp.name)
	return nil
}

// Stop 优雅关闭服务 (内部组件)
func (tp *TaskProcessor) Stop(ctx context.Context) error {
	fmt.Printf("[%s] Shutting down task processor...\n", tp.name)

	tp.mu.Lock()
	tp.running = false
	tp.mu.Unlock()

	// 关闭任务队列，不再接收新任务
	close(tp.taskQueue)

	// 等待所有工作协程完成当前任务
	done := make(chan struct{})
	go func() {
		tp.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		fmt.Printf("[%s] Task processor shutdown completed\n", tp.name)
		return nil
	case <-ctx.Done():
		fmt.Printf("[%s] Task processor shutdown timeout\n", tp.name)
		return ctx.Err()
	}
}

// GetName 返回处理器名称
func (tp *TaskProcessor) GetName() string {
	return tp.name
}

// worker 工作协程，处理任务队列中的任务
func (tp *TaskProcessor) worker(ctx context.Context, workerID int) {
	defer tp.wg.Done()

	fmt.Printf("[%s] Worker %d started\n", tp.name, workerID)
	defer fmt.Printf("[%s] Worker %d stopped\n", tp.name, workerID)

	for {
		select {
		case <-ctx.Done():
			fmt.Printf("[%s] Worker %d received shutdown signal\n", tp.name, workerID)
			return
		case task, ok := <-tp.taskQueue:
			if !ok {
				fmt.Printf("[%s] Worker %d: task queue closed\n", tp.name, workerID)
				return
			}
			tp.processTask(workerID, task)
		}
	}
}

// processTask 处理单个任务
func (tp *TaskProcessor) processTask(workerID int, task Task) {
	start := time.Now()
	fmt.Printf("[%s] Worker %d processing task %d: %s (priority: %d)\n",
		tp.name, workerID, task.ID, task.Data, task.Priority)

	// 模拟任务处理时间 (1-3 秒)
	processingTime := time.Duration(rand.Intn(3000)+1000) * time.Millisecond
	time.Sleep(processingTime)

	elapsed := time.Since(start)
	fmt.Printf("[%s] Worker %d completed task %d in %v\n",
		tp.name, workerID, task.ID, elapsed)
}

// taskGenerator 任务生成器，模拟外部任务输入
func (tp *TaskProcessor) taskGenerator(ctx context.Context) {
	defer tp.wg.Done()

	fmt.Printf("[%s] Task generator started\n", tp.name)
	defer fmt.Printf("[%s] Task generator stopped\n", tp.name)

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	taskID := 1
	for {
		select {
		case <-ctx.Done():
			fmt.Printf("[%s] Task generator received shutdown signal\n", tp.name)
			return
		case <-ticker.C:
			tp.mu.RLock()
			running := tp.running
			tp.mu.RUnlock()

			if !running {
				return
			}

			task := Task{
				ID:       taskID,
				Data:     fmt.Sprintf("Task-%d-Data", taskID),
				Priority: rand.Intn(10) + 1,
				Created:  time.Now(),
			}

			select {
			case tp.taskQueue <- task:
				fmt.Printf("[%s] Generated task %d\n", tp.name, taskID)
				taskID++
			default:
				fmt.Printf("[%s] Task queue full, dropping task %d\n", tp.name, taskID)
			}
		}
	}
}

// AddTask 外部接口，用于添加任务
func (tp *TaskProcessor) AddTask(task Task) bool {
	tp.mu.RLock()
	running := tp.running
	tp.mu.RUnlock()

	if !running {
		return false
	}

	select {
	case tp.taskQueue <- task:
		return true
	default:
		return false // 队列满了
	}
}

// GetQueueSize 获取当前队列大小
func (tp *TaskProcessor) GetQueueSize() int {
	return len(tp.taskQueue)
}
