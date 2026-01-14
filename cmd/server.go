/*
Copyright © 2026 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"curve/app"
	"curve/core"

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
		server := core.NewServer(serverAddr, handler)
		server.Start()
	},
}

func init() {
	rootCmd.AddCommand(serverCmd)

	serverCmd.Flags().StringVarP(&serverAddr, "addr", "a", "localhost:8080", "Server address to listen on")
}
