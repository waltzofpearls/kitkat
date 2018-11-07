package command

import (
	"os"

	"github.com/spf13/cobra"
	"github.com/waltzofpearls/kitkat/producer"
)

func init() {
	p := producer.New()
	cmd := &cobra.Command{
		Use:     "produce",
		Short:   "Producer mode. Writes messages to given kinesis stream(s)",
		Aliases: []string{"p"},
		Run: func(cmd *cobra.Command, args []string) {
			if !cmd.Flags().Changed("stream") {
				cmd.Help()
				os.Exit(1)
			}
			p.Write()
		},
	}
	rootCmd.AddCommand(cmd)
	cmd.Flags().StringVarP(&p.Stream, "stream", "t", "", "Kinesis stream name")
}
