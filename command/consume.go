package command

import (
	"fmt"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
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
	cmd.Flags().StringVarP(&c.Stream, "stream", "s", "", "Kinesis stream name")
	cmd.Flags().StringVarP(&c.Region, "region", "r", "us-west-2", "AWS region")
	cmd.Flags().StringVarP(&c.Iterator, "iterator", "I", "LATEST", "Iterator type: LATEST, TRIM_HORIZON, AT_SEQUENCE_NUMBER or AT_TIMESTAMP")
	cmd.Flags().Int64VarP(&c.Limit, "limit", "l", 500, "Limit records length of each GetRecords request")
	cmd.Flags().Int64VarP(&c.Interval, "interval", "i", 100, "Interval in milliseconds between each GetRecords request")
	cmd.Flags().StringVarP(&c.Since, "since", "t", "", "Show records since timestamp (e.g. 2016-04-20T12:00:00+09:00) when iterator type is AT_TIMESTAMP")
}

func consume(c *consumer.Consumer) runFunc {
	return func(cmd *cobra.Command, args []string) {
		if !cmd.Flags().Changed("stream") {
			cmd.Help()
			os.Exit(1)
		}
		c.Verbose = verbose
		c.Client = kinesis.New(
			session.New(&aws.Config{
				Region: aws.String(c.Region),
			}),
		)
		if err := c.Read(); err != nil {
			fmt.Fprintln(os.Stderr, "ERROR:", err)
			os.Exit(1)
		}
	}
}
