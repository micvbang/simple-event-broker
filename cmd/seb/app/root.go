package app

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "seb",
	Short: "Seb",
	Long:  `Seb is a Simple Event Broker that has the goals of keeping your data safe, being cheap to run and easy to manage`,
	Run: func(cmd *cobra.Command, args []string) {
		cmd.Help()
	},
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func init() {
	// root
	rootCmd.AddCommand(serveCmd)
	rootCmd.AddCommand(readCmd)
	rootCmd.AddCommand(benchmarkCmd)
	rootCmd.AddCommand(benchmarkReadCmd)
	rootCmd.AddCommand(clientCmd)
	rootCmd.AddCommand(offsetsCmd)

	// client
	clientCmd.AddCommand(clientGetCmd)

	// offsets
	offsetsCmd.AddCommand(offsetsWriteCmd)
	offsetsCmd.AddCommand(offsetsListCmd)
	offsetsCmd.AddCommand(offsetsReadCmd)
}
