/*
Copyright © 2026 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"curve/app"
	"curve/core"
	"fmt"
	"log"

	"github.com/spf13/cobra"
)

var serverAddr string

// serverCmd represents the server command
var serverCmd = &cobra.Command{
	Use:   "server",
	Short: "Start the curve server",
	Long: `Start the curve server to accept client connections.
The server listens on the specified address and handles incoming commands.`,
	Run: func(cmd *cobra.Command, args []string) {
		handler := app.NewMplHandler()
		server := core.NewServer()
		server.SetHandler(handler)

		if err := server.Listen("tcp", serverAddr); err != nil {
			log.Fatalf("Failed to listen on %s: %v", serverAddr, err)
		}

		fmt.Printf("Server listening on %s\n", serverAddr)
		if err := server.Serve(); err != nil {
			log.Fatalf("Server error: %v", err)
		}
	},
}

func init() {
	rootCmd.AddCommand(serverCmd)

	serverCmd.Flags().StringVarP(&serverAddr, "addr", "a", "localhost:8080", "Server address to listen on")
}
