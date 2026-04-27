package app

import (
	"fmt"
	"os"

	"github.com/micvbang/simple-event-broker/internal/seboffsets"
	"github.com/spf13/cobra"
)

var offsetsReadFlags OffsetsReadFlags

func init() {
	fs := offsetsReadCmd.Flags()

	fs.StringVarP(&offsetsReadFlags.path, "path", "p", "", "Path to .offsets file on disk")

	offsetsReadCmd.MarkFlagRequired("path")
}

var offsetsReadCmd = &cobra.Command{
	Use:   "read",
	Short: "Read an offsets file from disk",
	Long:  "Read an .offsets file from local disk using seboffsets and print its contents",
	RunE: func(cmd *cobra.Command, args []string) error {
		flags := offsetsReadFlags

		f, err := os.Open(flags.path)
		if err != nil {
			return fmt.Errorf("opening %q: %w", flags.path, err)
		}
		defer f.Close()

		offsets, err := seboffsets.Parse(f)
		if err != nil {
			return fmt.Errorf("parsing %q: %w", flags.path, err)
		}

		fmt.Printf("Total offsets: %d\n", len(offsets))
		for _, offset := range offsets {
			fmt.Println(offset)
		}

		return nil
	},
}

type OffsetsReadFlags struct {
	path string
}
