package app

import (
	"github.com/spf13/cobra"
)

var clientCmd = &cobra.Command{
	Use:   "client",
	Short: "Use Seb client",
	Long:  "Use Seb client to send requests to Seb instance",
}
