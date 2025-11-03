package main

import (
	"fmt"
	"math/rand"
	"time"

	"curve/app"
	"curve/core"
)

func sendPacket(client *app.Client, stream uint32, count int, ch chan int) {
	var err error
	defer func() {
		ch <- count
	}()
	commands := []app.Command{
		{
			Action: "ping",
			Params: map[string]any{"stream": stream, "value": "hello"},
		},
		{
			Action: "upload",
			Params: map[string]any{"stream": stream, "value": "upload"},
		},
		{
			Action: "download",
			Params: map[string]any{"stream": stream, "value": "download"},
		},
	}
	for _, cmd := range commands {
		fmt.Printf("Sending command: %+v\n", cmd)
		err = client.SendCommand(stream, &cmd)
		if err != nil {
			fmt.Println("Error sending packet:", err)
			return
		}
		time.Sleep(time.Duration(rand.Intn(10)+1) * time.Second)
	}
}

func main() {
	// 启动服务器
	handler := app.NewMplHandler()
	server := core.NewServer("127.0.0.1:8080", handler)
	go server.Start()

	// 创建客户端
	client := app.NewClient("localhost:8080")
	if client == nil {
		fmt.Println("Error creating client")
		return
	}
	defer client.Close()

	// 发送数据
	ch := make(chan int)
	go sendPacket(client, 1, 10, ch)
	go sendPacket(client, 2, 10, ch)
	<-ch
	<-ch
	time.Sleep(2 * time.Second)
	fmt.Println("done")
}
