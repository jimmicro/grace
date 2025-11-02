package main

import (
	"context"
	"time"

	"github.com/jimmicro/grace"
)

func main() {
	// 创建演示服务，包含多个异步任务处理器
	demoService := NewDemoService("MultiProcessorDemo")

	// 创建其他可能的服务（演示多服务场景）
	services := []grace.Grace{
		demoService,
		// 这里可以添加其他服务，比如 HTTP 服务、数据库服务等
	}

	// 创建 Shepherd
	shepherd := grace.NewShepherd(services,
		grace.WithTimeout(20*time.Second),
	)

	// 启动服务
	shepherd.Start(context.Background())
}
