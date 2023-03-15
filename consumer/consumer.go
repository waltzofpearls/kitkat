package consumer

import (
	"bytes"
	"compress/zlib"
	"crypto/md5"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/kinesis/kinesisiface"
	"github.com/olekukonko/tablewriter"
	"github.com/waltzofpearls/kitkat/aggregated"
	"google.golang.org/protobuf/proto"
)

var magicNumber = []byte{0xF3, 0x89, 0x9A, 0xC2}

type Consumer struct {
	Stream   string
	Region   string
	Profile  string
	Iterator string
	Limit    int64
	Interval int64
	Since    string
	Compress string
	Verbose  bool

	Client kinesisiface.KinesisAPI

	out     io.Writer
	errChan chan error
}

func New() *Consumer {
	return &Consumer{
		out:     os.Stdout,
		errChan: make(chan error, 1),
	}
}

func (c *Consumer) Read() error {
	atTimestamp, err := c.timestamp()
	if err != nil {
		return fmt.Errorf("--since needs to be in RFC3339 format. %s", err)
	}

	for _, stream := range strings.Split(c.Stream, ",") {
		c.readOneStream(stream, atTimestamp)
	}

	select {
	case err := <-c.errChan:
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

func (c *Consumer) readOneStream(stream string, atTimestamp time.Time) {
	kinesisStream, err := c.Client.DescribeStream(&kinesis.DescribeStreamInput{
		StreamName: aws.String(stream),
	})
	if err != nil {
		c.errChan <- fmt.Errorf("failed to describe stream. %s", err)
		return
	}
	c.printStreamInfo(kinesisStream)
	for _, shard := range kinesisStream.StreamDescription.Shards {
		go c.readOneShard(stream, shard, atTimestamp)
	}
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
	table := tablewriter.NewWriter(c.out)
	for _, v := range data {
		table.Append(v)
	}
	table.Render()
}

func (c *Consumer) closed(shard *kinesis.Shard) bool {
	return shard.SequenceNumberRange.EndingSequenceNumber != nil
}

func (c *Consumer) readOneShard(stream string, shard *kinesis.Shard, atTimestamp time.Time) {
	if c.closed(shard) {
		return
	}

	iteratorOutput, err := c.Client.GetShardIterator(&kinesis.GetShardIteratorInput{
		ShardId:           shard.ShardId,
		ShardIteratorType: aws.String(c.Iterator),
		StreamName:        aws.String(stream),
		Timestamp:         aws.Time(atTimestamp),
	})
	if err != nil {
		c.errChan <- fmt.Errorf("failed to get iterator. %s", err)
		return
	}
	iterator := iteratorOutput.ShardIterator
	for {
		iterator, err = c.printRecords(iterator, stream, shard)
		if err != nil {
			c.errChan <- fmt.Errorf(
				"failed to get records from stream %s and shard %s. %s",
				stream, *shard.ShardId, err)
			return
		}
		time.Sleep(time.Duration(c.Interval) * time.Millisecond)
	}
}

func (c *Consumer) printRecords(iterator *string, stream string, shard *kinesis.Shard) (*string, error) {
	records, err := c.Client.GetRecords(&kinesis.GetRecordsInput{
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
				printOneRecord(c.out, stream, shard, r, c.Compress, c.Verbose)
			}
		} else {
			printOneRecord(c.out, stream, shard, record, c.Compress, c.Verbose)
		}
	}
	return records.NextShardIterator, nil
}

var isAggregated = func(record *kinesis.Record) bool {
	return bytes.HasPrefix(record.Data, magicNumber)
}

var deaggregate = func(record *kinesis.Record) []*kinesis.Record {
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

const recordDatetimeFormat = "2006-01-02 15:04:05"

var printOneRecord = func(out io.Writer, stream string, shard *kinesis.Shard, record *kinesis.Record, compress string, verbose bool) {
	var data []byte
	if compress == "zlib" {
		data = decompressZlib(record.Data)
	} else {
		data = record.Data
	}
	datetime := record.ApproximateArrivalTimestamp.Format(recordDatetimeFormat)
	message := string(bytes.TrimSuffix(data, []byte("\n")))
	if verbose {
		fmt.Fprintln(out, datetime, stream, *shard.ShardId, *record.SequenceNumber, message)
	} else {
		fmt.Fprintln(out, datetime, message)
	}
}

var decompressZlib = func(data []byte) []byte {
	b := bytes.NewReader(data)
	z, err := zlib.NewReader(b)
	if err != nil {
		return data
	}
	defer z.Close()

	out, err := ioutil.ReadAll(z)
	if err != nil {
		return data
	}
	return out
}
