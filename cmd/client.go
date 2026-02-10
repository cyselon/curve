/*
Copyright © 2026 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"

	"curve/app"
)

var clientServerAddr string

// clientCmd represents the client command
var clientCmd = &cobra.Command{
	Use:   "client",
	Short: "Start the curve client",
	Long: `Start the curve client to connect to a server.
The client connects to the specified server address and can send commands.`,
	Run: func(cmd *cobra.Command, args []string) {
		client, err := app.NewClient(clientServerAddr)
		if err != nil {
			fmt.Printf("Failed to create client: %v\n", err)
			os.Exit(1)
		}
		defer client.Close()

		fmt.Printf("Connected to server at %s\n", clientServerAddr)
		fmt.Println("Client is ready. Type 'help' for available commands, 'exit' or 'quit' to exit.")
		fmt.Println()

		// 启动交互式命令循环
		runInteractiveLoop(client)
	},
}

// runInteractiveLoop 运行交互式命令循环
func runInteractiveLoop(client *app.Client) {
	scanner := bufio.NewScanner(os.Stdin)

	for {
		fmt.Print("curve> ")
		if !scanner.Scan() {
			break
		}

		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		// 解析命令
		parts := strings.Fields(line)
		command := parts[0]

		// 处理内置命令
		switch strings.ToLower(command) {
		case "exit", "quit":
			fmt.Println("Goodbye!")
			return
		case "help":
			printHelp()
			continue
		default:
			// 发送命令到服务器
			cmd := parseCommand(parts)
			if err := client.SendCommand(cmd); err != nil {
				fmt.Printf("Error sending command: %v\n", err)
			} else {
				fmt.Printf("Command '%s' sent successfully\n", command)
			}
		}
	}

	if err := scanner.Err(); err != nil {
		fmt.Printf("Error reading input: %v\n", err)
	}
}

// parseCommand 解析用户输入的命令
func parseCommand(parts []string) *app.Command {
	if len(parts) == 0 {
		return nil
	}

	cmd := &app.Command{
		Action: parts[0],
		Params: make(map[string]interface{}),
	}

	// 解析参数（格式：key=value）
	for i := 1; i < len(parts); i++ {
		part := parts[i]
		if strings.Contains(part, "=") {
			kv := strings.SplitN(part, "=", 2)
			if len(kv) == 2 {
				cmd.Params[kv[0]] = kv[1]
			}
		} else {
			// 如果没有 =，作为位置参数
			cmd.Params[fmt.Sprintf("arg%d", i)] = part
		}
	}

	return cmd
}

// printHelp 打印帮助信息
func printHelp() {
	fmt.Println("Available commands:")
	fmt.Println("  ping                    - Send a ping command to the server")
	fmt.Println("  upload [params]         - Send an upload command")
	fmt.Println("  <command> [key=value]   - Send a custom command with parameters")
	fmt.Println("  help                    - Show this help message")
	fmt.Println("  exit, quit              - Exit the client")
	fmt.Println()
	fmt.Println("Examples:")
	fmt.Println("  curve> ping")
	fmt.Println("  curve> upload file=test.txt size=1024")
	fmt.Println("  curve> custom_action param1=value1 param2=value2")
}

func init() {
	rootCmd.AddCommand(clientCmd)

	clientCmd.Flags().StringVarP(&clientServerAddr, "server", "s", "localhost:8080", "Server address to connect to")
}
