/*
Copyright © 2026 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"curve/app"
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var clientServerAddr string

// clientCmd represents the client command
var clientCmd = &cobra.Command{
	Use:   "client",
	Short: "Start the curve client",
	Long: `Start the curve client to connect to a server.
The client connects to the specified server address and can send commands.`,
	Run: func(cmd *cobra.Command, args []string) {
		client := app.NewClient(clientServerAddr)
		if client == nil {
			fmt.Println("Failed to create client")
			os.Exit(1)
		}
		defer client.Close()

		fmt.Printf("Connected to server at %s\n", clientServerAddr)
		fmt.Println("Client is ready. Use the client API to send commands.")
	},
}

func init() {
	rootCmd.AddCommand(clientCmd)

	clientCmd.Flags().StringVarP(&clientServerAddr, "server", "s", "localhost:8080", "Server address to connect to")
}
