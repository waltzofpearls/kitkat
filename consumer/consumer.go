package consumer

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/olekukonko/tablewriter"
)

type Consumer struct {
	Stream   string
	Region   string
	Size     int
	Iterator string
	Limit    int64
	Interval time.Duration
	Since    string
	Verbose  bool

	client *kinesis.Kinesis
}

func New() *Consumer {
	return new(Consumer)
}

func (c *Consumer) Read() error {
	c.client = kinesis.New(
		session.New(&aws.Config{
			Region: aws.String(c.Region),
		}),
	)
	atTimestamp, err := c.timestamp()
	if err != nil {
		return fmt.Errorf("--since needs to be in RFC3339 format. %s", err)
	}

	stream, err := c.client.DescribeStream(&kinesis.DescribeStreamInput{
		StreamName: aws.String(c.Stream),
	})
	if err != nil {
		return fmt.Errorf("failed to describe stream. %s", err)
	}
	c.printStreamInfo(stream)

	errChan := make(chan error)
	for _, shard := range stream.StreamDescription.Shards {
		go c.iterateFrom(shard, atTimestamp, errChan)
	}
	select {
	case err := <-errChan:
		return err
	}
}

func (c *Consumer) timestamp() (time.Time, error) {
	var (
		timestamp time.Time
		err       error
	)
	if c.Iterator == "AT_TIMESTAMP" && c.Since != "" {
		timestamp, err = time.Parse(time.RFC3339, c.Since)
	}
	return timestamp, err
}

func (c *Consumer) printStreamInfo(stream *kinesis.DescribeStreamOutput) {
	if !c.Verbose {
		return
	}
	desc := stream.StreamDescription
	var active, closed int
	for _, s := range desc.Shards {
		if s.SequenceNumberRange.EndingSequenceNumber != nil {
			closed += 1
		} else {
			active += 1
		}
	}
	data := [][]string{
		[]string{"Stream name:", *desc.StreamName},
		[]string{"Created at:", (*desc.StreamCreationTimestamp).Format(time.RFC1123)},
		[]string{"Encryption:", *desc.EncryptionType},
		[]string{"Retention:", fmt.Sprintf("%d hours", *desc.RetentionPeriodHours)},
		[]string{"Active:", fmt.Sprintf("%d shards", active)},
		[]string{"Closed:", fmt.Sprintf("%d shards", closed)},
	}
	table := tablewriter.NewWriter(os.Stdout)
	for _, v := range data {
		table.Append(v)
	}
	table.Render()
}

func (c *Consumer) iterateFrom(shard *kinesis.Shard, atTimestamp time.Time, errChan chan<- error) {
	if shard.SequenceNumberRange.EndingSequenceNumber != nil {
		return
	}

	iteratorOutput, err := c.client.GetShardIterator(&kinesis.GetShardIteratorInput{
		ShardId:           shard.ShardId,
		ShardIteratorType: aws.String(c.Iterator),
		StreamName:        aws.String(c.Stream),
		Timestamp:         aws.Time(atTimestamp),
	})
	if err != nil {
		errChan <- fmt.Errorf("failed to get iterator. %s", err)
		return
	}
	iterator := iteratorOutput.ShardIterator
	for {
		iterator, err = c.getRecordsBy(iterator, shard)
		if err != nil {
			errChan <- fmt.Errorf(
				"failed to get records from stream %s and shard %s. %s",
				c.Stream, shard.ShardId, err)
			return
		}
		time.Sleep(c.Interval)
	}
}

func (c *Consumer) getRecordsBy(iterator *string, shard *kinesis.Shard) (*string, error) {
	records, err := c.client.GetRecords(&kinesis.GetRecordsInput{
		ShardIterator: iterator,
		Limit:         &c.Limit,
	})
	if err != nil {
		return nil, err
	}
	var data []byte
	for _, r := range records.Records {
		if c.Size > 0 && len(r.Data) > c.Size {
			data = r.Data[:c.Size-1]
		} else {
			data = r.Data[:]
		}
		datetime := r.ApproximateArrivalTimestamp.Format("2006-01-02 15:04:05")
		message := strings.TrimSuffix(string(data), "\n")
		if c.Verbose {
			fmt.Println(datetime, *shard.ShardId, *r.SequenceNumber, message)
		} else {
			fmt.Println(datetime, message)
		}
	}
	return records.NextShardIterator, nil
}
