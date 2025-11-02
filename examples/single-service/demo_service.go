package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/jimmicro/grace"
)

// DemoService 演示服务，包含多个异步任务处理器
type DemoService struct {
	name       string
	processors []*TaskProcessor
	wg         sync.WaitGroup
	running    bool
	mu         sync.RWMutex
	taskID     int // 任务 ID 计数器
}

// NewDemoService 创建新的演示服务
func NewDemoService(name string) *DemoService {
	ds := &DemoService{
		name: name,
	}

	ds.processors = []*TaskProcessor{
		NewTaskProcessor("EmailProcessor", 2, 20),
		NewTaskProcessor("ImageProcessor", 4, 10),
		NewTaskProcessor("DataProcessor", 3, 15),
		NewTaskProcessor("NotificationProcessor", 1, 5),
	}

	return ds
}

func (ds *DemoService) AddService(ctx context.Context, service func(ctx context.Context)) {
	ds.wg.Add(1)
	go func() {
		defer ds.wg.Done()
		service(ctx)
	}()
}

// runWithTicker 通用的带定时器的服务运行方法
func (ds *DemoService) runWithTicker(ctx context.Context, name string, interval time.Duration, handler func() error) {
	fmt.Printf("[%s] %s started\n", ds.name, name)
	defer fmt.Printf("[%s] %s stopped\n", ds.name, name)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			ds.mu.RLock()
			running := ds.running
			ds.mu.RUnlock()

			if !running {
				return
			}

			if err := handler(); err != nil {
				fmt.Printf("[%s] %s error: %v\n", ds.name, name, err)
			}
		}
	}
}

// Run 启动服务 (实现 Grace 接口)
func (ds *DemoService) Run(ctx context.Context) error {
	ds.mu.Lock()
	ds.running = true
	ds.mu.Unlock()

	fmt.Printf("[%s] Starting demo service with %d processors\n", ds.name, len(ds.processors))

	// start all processors
	for _, processor := range ds.processors {
		ds.wg.Add(1)
		go func(p *TaskProcessor) {
			defer ds.wg.Done()
			if err := p.Start(ctx); err != nil {
				fmt.Printf("[%s] Processor %s error: %v\n", ds.name, p.GetName(), err)
			}
		}(processor)
	}

	// start task distributor
	ds.AddService(ctx, func(ctx context.Context) {
		ds.runWithTicker(ctx, "Task distributor", 3*time.Second, ds.distributeTask)
	})

	// start monitor
	ds.AddService(ctx, func(ctx context.Context) {
		ds.runWithTicker(ctx, "Monitor", 10*time.Second, ds.printStatus)
	})

	// start manual task adder
	ds.AddService(ctx, ds.startManualTaskAdder)

	// wait for all components to complete
	ds.wg.Wait()
	fmt.Printf("[%s] Demo service stopped\n", ds.name)
	return nil
}

// Shutdown 优雅关闭服务 (实现 Grace 接口)
func (ds *DemoService) Shutdown(ctx context.Context) error {
	fmt.Printf("[%s] Shutting down demo service...\n", ds.name)

	ds.mu.Lock()
	ds.running = false
	ds.mu.Unlock()

	// 关闭所有处理器
	var shutdownWg sync.WaitGroup
	for _, processor := range ds.processors {
		shutdownWg.Add(1)
		go func(p *TaskProcessor) {
			defer shutdownWg.Done()
			if err := p.Stop(ctx); err != nil {
				fmt.Printf("[%s] Error stopping processor %s: %v\n", ds.name, p.GetName(), err)
			}
		}(processor)
	}

	// 等待所有处理器关闭或超时
	done := make(chan struct{})
	go func() {
		shutdownWg.Wait()
		ds.wg.Wait() // wait for all services to complete
		close(done)
	}()

	select {
	case <-done:
		fmt.Printf("[%s] Demo service shutdown completed\n", ds.name)
		return nil
	case <-ctx.Done():
		fmt.Printf("[%s] Demo service shutdown timeout\n", ds.name)
		return ctx.Err()
	}
}

// Name 返回服务名称 (实现 Grace 接口)
func (ds *DemoService) Name() string {
	return ds.name
}

// distributeTask 分发任务的具体逻辑
func (ds *DemoService) distributeTask() error {
	// 使用静态变量保持任务 ID 状态
	taskID := ds.getNextTaskID()

	// 随机选择一个处理器并分发任务
	if len(ds.processors) > 0 {
		processorIndex := (taskID - 1) % len(ds.processors)
		processor := ds.processors[processorIndex]

		task := Task{
			ID:       taskID,
			Data:     fmt.Sprintf("Distributed-Task-%d", taskID),
			Priority: (taskID % 10) + 1,
			Created:  time.Now(),
		}

		if processor.AddTask(task) {
			fmt.Printf("[%s] Distributed task %d to %s\n", ds.name, taskID, processor.GetName())
		} else {
			fmt.Printf("[%s] Failed to distribute task %d to %s (queue full)\n", ds.name, taskID, processor.GetName())
		}
	}

	return nil
}

// printStatus 打印状态的具体逻辑
func (ds *DemoService) printStatus() error {
	// 输出各处理器的队列状态
	fmt.Printf("[%s] === Processor Status ===\n", ds.name)
	for _, processor := range ds.processors {
		queueSize := processor.GetQueueSize()
		fmt.Printf("[%s] %s: queue size = %d\n", ds.name, processor.GetName(), queueSize)
	}
	fmt.Printf("[%s] ========================\n", ds.name)
	return nil
}

// getNextTaskID 获取下一个任务 ID
func (ds *DemoService) getNextTaskID() int {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	ds.taskID++
	return ds.taskID
}

// AddTaskToProcessor 外部接口，向指定处理器添加任务
func (ds *DemoService) AddTaskToProcessor(processorName string, task Task) bool {
	ds.mu.RLock()
	running := ds.running
	ds.mu.RUnlock()

	if !running {
		return false
	}

	for _, processor := range ds.processors {
		if processor.GetName() == processorName {
			return processor.AddTask(task)
		}
	}
	return false
}

// GetProcessorNames 获取所有处理器名称
func (ds *DemoService) GetProcessorNames() []string {
	names := make([]string, len(ds.processors))
	for i, processor := range ds.processors {
		names[i] = processor.GetName()
	}
	return names
}

// startManualTaskAdder 启动手动任务添加器
func (ds *DemoService) startManualTaskAdder(ctx context.Context) {

	// 等待服务完全启动
	select {
	case <-time.After(5 * time.Second):
		// 继续执行
	case <-ctx.Done():
		return
	}

	fmt.Println("[MANUAL] Starting to add specific tasks...")

	// 获取处理器名称
	processorNames := ds.GetProcessorNames()
	fmt.Printf("[MANUAL] Available processors: %v\n", processorNames)

	// 向不同的处理器添加特定任务
	tasks := []struct {
		processor string
		taskType  string
		count     int
	}{
		{"EmailProcessor", "Email", 3},
		{"ImageProcessor", "Image", 2},
		{"DataProcessor", "Data", 4},
		{"NotificationProcessor", "Notification", 2},
	}

	for _, taskGroup := range tasks {
		for i := 1; i <= taskGroup.count; i++ {
			// 检查是否需要停止
			select {
			case <-ctx.Done():
				fmt.Println("[MANUAL] Manual task adder stopped")
				return
			default:
			}

			task := Task{
				ID:       2000 + i,
				Data:     fmt.Sprintf("Manual-%s-Task-%d", taskGroup.taskType, i),
				Priority: 8, // 高优先级
				Created:  time.Now(),
			}

			if ds.AddTaskToProcessor(taskGroup.processor, task) {
				fmt.Printf("[MANUAL] Added %s task %d to %s\n", taskGroup.taskType, task.ID, taskGroup.processor)
			} else {
				fmt.Printf("[MANUAL] Failed to add %s task %d to %s\n", taskGroup.taskType, task.ID, taskGroup.processor)
			}

			// 可中断的等待
			select {
			case <-time.After(300 * time.Millisecond):
				// 继续下一个任务
			case <-ctx.Done():
				fmt.Println("[MANUAL] Manual task adder stopped")
				return
			}
		}
	}

	fmt.Println("[MANUAL] Finished adding manual tasks")
}

var _ grace.Grace = (*DemoService)(nil) // 确保实现了 Grace 接口
