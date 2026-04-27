package app

import "github.com/spf13/cobra"

var offsetsCmd = &cobra.Command{
	Use:   "offsets",
	Short: "Manage record batch offsets",
	Long:  "Manage Seb record batch offsets stored in the configured backing storage",
}
