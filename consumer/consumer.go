package consumer

import (
	"bytes"
	"crypto/md5"
	"fmt"
	"os"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/golang/protobuf/proto"
	"github.com/olekukonko/tablewriter"
	"github.com/waltzofpearls/kitkat/aggregated"
)

var magicNumber = []byte{0xF3, 0x89, 0x9A, 0xC2}

type Consumer struct {
	Stream   string
	Region   string
	Iterator string
	Limit    int64
	Interval int64
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
		go c.read(shard, atTimestamp, errChan)
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
		if c.closed(s) {
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

func (c *Consumer) closed(shard *kinesis.Shard) bool {
	return shard.SequenceNumberRange.EndingSequenceNumber != nil
}

func (c *Consumer) read(shard *kinesis.Shard, atTimestamp time.Time, errChan chan<- error) {
	if c.closed(shard) {
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
		time.Sleep(time.Duration(c.Interval) * time.Millisecond)
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
	for _, record := range records.Records {
		if isAggregated(record) {
			deaggregated := deaggregate(record)
			for _, r := range deaggregated {
				printRecord(shard, r, c.Verbose)
			}
		} else {
			printRecord(shard, record, c.Verbose)
		}
	}
	return records.NextShardIterator, nil
}

func isAggregated(record *kinesis.Record) bool {
	return bytes.HasPrefix(record.Data, magicNumber)
}

func deaggregate(record *kinesis.Record) []*kinesis.Record {
	src := record.Data[len(magicNumber) : len(record.Data)-md5.Size]
	dest := new(aggregated.AggregatedRecord)
	err := proto.Unmarshal(src, dest)
	if err != nil {
		return []*kinesis.Record{}
	}
	records := make([]*kinesis.Record, len(dest.Records))
	for i, r := range dest.Records {
		records[i] = &kinesis.Record{
			ApproximateArrivalTimestamp: record.ApproximateArrivalTimestamp,
			Data:                        r.GetData(),
			EncryptionType:              record.EncryptionType,
			PartitionKey:                &dest.PartitionKeyTable[r.GetPartitionKeyIndex()],
			SequenceNumber:              record.SequenceNumber,
		}
	}
	return records
}

func printRecord(shard *kinesis.Shard, record *kinesis.Record, verbose bool) {
	datetime := record.ApproximateArrivalTimestamp.Format("2006-01-02 15:04:05")
	message := string(bytes.TrimSuffix(record.Data, []byte("\n")))
	if verbose {
		fmt.Println(datetime, *shard.ShardId, *record.SequenceNumber, message)
	} else {
		fmt.Println(datetime, message)
	}
}
