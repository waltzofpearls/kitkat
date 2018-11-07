package command

import (
	"fmt"
	"os"
	"time"

	"github.com/spf13/cobra"
	"github.com/waltzofpearls/kitkat/consumer"
)

func init() {
	c := consumer.New()
	cmd := &cobra.Command{
		Use:     "consume",
		Short:   "Consumer mode. Reads messages from given kinesis stream(s)",
		Aliases: []string{"c"},
		Run:     consume(c),
	}
	rootCmd.AddCommand(cmd)
	cmd.Flags().StringVarP(&c.Stream, "stream", "t", "", "Kinesis stream name")
	cmd.Flags().StringVarP(&c.Region, "region", "r", "us-west-2", "AWS region")
	cmd.Flags().IntVarP(&c.Size, "size", "z", 0, "Print the given size of bytes per item, 0 prints everything")
	cmd.Flags().StringVarP(&c.Iterator, "iterator", "i", "LATEST", "Iterator type: LATEST, TRIM_HORIZON, AT_SEQUENCE_NUMBER or AT_TIMESTAMP")
	cmd.Flags().Int64VarP(&c.Limit, "limit", "l", 100, "Limit records length of each GetRecords request")
	cmd.Flags().DurationVarP(&c.Interval, "interval", "n", 3*time.Second, "Interval between each GetRecords request")
	cmd.Flags().StringVarP(&c.Since, "since", "s", "", "Show records since timestamp (e.g. 2016-04-20T12:00:00+09:00) when iterator type is AT_TIMESTAMP")
}

func consume(c *consumer.Consumer) runFunc {
	return func(cmd *cobra.Command, args []string) {
		if !cmd.Flags().Changed("stream") {
			cmd.Help()
			os.Exit(1)
		}
		c.Verbose = verbose
		if err := c.Read(); err != nil {
			fmt.Fprintln(os.Stderr, "ERROR:", err)
			os.Exit(1)
		}
	}
}
