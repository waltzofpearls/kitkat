package command

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var (
	rootCmd = &cobra.Command{
		Use:   "kitkat",
		Short: "Kitkat is a kinesis stream producer and consumer",
		Run: func(cmd *cobra.Command, args []string) {
			cmd.Help()
		},
	}
	verbose bool
)

type runFunc func(cmd *cobra.Command, args []string)

func init() {
	rootCmd.PersistentFlags().BoolVarP(&verbose, "verbose", "v", false, "verbose output")
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
