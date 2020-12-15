package consumer

import (
	"bytes"
	"crypto/md5"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/kinesis/kinesisiface"
	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/waltzofpearls/kitkat/aggregated"
)

type mockKinesis struct {
	kinesisiface.KinesisAPI
	describeStreamFunc   func(*kinesis.DescribeStreamInput) (*kinesis.DescribeStreamOutput, error)
	getShardIteratorFunc func(*kinesis.GetShardIteratorInput) (*kinesis.GetShardIteratorOutput, error)
	getRecordsFunc       func(*kinesis.GetRecordsInput) (*kinesis.GetRecordsOutput, error)
}

func (k *mockKinesis) DescribeStream(input *kinesis.DescribeStreamInput) (*kinesis.DescribeStreamOutput, error) {
	return k.describeStreamFunc(input)
}

func (k *mockKinesis) GetShardIterator(input *kinesis.GetShardIteratorInput) (*kinesis.GetShardIteratorOutput, error) {
	return k.getShardIteratorFunc(input)
}

func (k *mockKinesis) GetRecords(input *kinesis.GetRecordsInput) (*kinesis.GetRecordsOutput, error) {
	return k.getRecordsFunc(input)
}

func TestRead(t *testing.T) {
	fakeEndingSeqNum := "fake-ending-sequence-number"

	cases := []struct {
		name           string
		iterator       string
		since          string
		stream         string
		mockClient     kinesisiface.KinesisAPI
		expectAnyError bool
	}{
		{
			name:           "cannot parse timestamp",
			iterator:       "AT_TIMESTAMP",
			since:          "invalid-timestamp",
			expectAnyError: true,
		},
		{
			name:     "cannot read from stream",
			iterator: "LATEST",
			stream:   "test-stream",
			mockClient: &mockKinesis{
				describeStreamFunc: func(input *kinesis.DescribeStreamInput) (*kinesis.DescribeStreamOutput, error) {
					return nil, errors.New("intended unit test error")
				},
			},
			expectAnyError: true,
		},
		{
			name:     "happy path",
			iterator: "LATEST",
			stream:   "test-stream",
			mockClient: &mockKinesis{
				describeStreamFunc: func(input *kinesis.DescribeStreamInput) (*kinesis.DescribeStreamOutput, error) {
					return &kinesis.DescribeStreamOutput{
						StreamDescription: &kinesis.StreamDescription{
							Shards: []*kinesis.Shard{
								&kinesis.Shard{
									SequenceNumberRange: &kinesis.SequenceNumberRange{
										EndingSequenceNumber: &fakeEndingSeqNum,
									},
								},
							},
						},
					}, nil
				},
			},
			expectAnyError: false,
		},
	}

	for _, test := range cases {
		c := New()
		c.Iterator = test.iterator
		c.Since = test.since
		c.Stream = test.stream
		c.Verbose = false
		c.Client = test.mockClient
		if !test.expectAnyError {
			c.errChan <- nil
		}
		err := c.Read()
		if test.expectAnyError {
			assert.Error(t, err, "test [%s] failed", test.name)
		} else {
			assert.NoError(t, err, "test [%s] failed", test.name)
		}
	}
}

func TestPrintStreamInfo(t *testing.T) {
	var buf bytes.Buffer
	c := New()
	c.Verbose = true
	c.out = &buf

	fakeStreamName := "fake-stream-name"
	fakeStreamCreationTimestamp := time.Now()
	fakeEncryptionType := "fake-encryption-type"
	fakeRetentionPeriodHours := int64(336)
	fakeEndingSeqNum := "fake-ending-sequence-number"

	c.printStreamInfo(&kinesis.DescribeStreamOutput{
		StreamDescription: &kinesis.StreamDescription{
			StreamName:              &fakeStreamName,
			StreamCreationTimestamp: &fakeStreamCreationTimestamp,
			EncryptionType:          &fakeEncryptionType,
			RetentionPeriodHours:    &fakeRetentionPeriodHours,
			Shards: []*kinesis.Shard{
				// 2 active shards
				&kinesis.Shard{
					SequenceNumberRange: &kinesis.SequenceNumberRange{},
				},
				&kinesis.Shard{
					SequenceNumberRange: &kinesis.SequenceNumberRange{},
				},
				// 1 closed shard
				&kinesis.Shard{
					SequenceNumberRange: &kinesis.SequenceNumberRange{
						EndingSequenceNumber: &fakeEndingSeqNum,
					},
				},
			},
		},
	})
	out := buf.String()

	assert.Regexp(t, "Stream name:.+"+fakeStreamName, out)
	assert.Regexp(t, "Created at:.+"+fakeStreamCreationTimestamp.Format(time.RFC1123), out)
	assert.Regexp(t, "Encryption:.+"+fakeEncryptionType, out)
	assert.Regexp(t, "Retention:.+336.+hours", out)
	assert.Regexp(t, "Active:.+2.+shards", out)
	assert.Regexp(t, "Closed:.+1.+shards", out)
}

func TestReadOneShard(t *testing.T) {
	fakeShardID := "fake-shard-id"
	fakeIterator := "fake-iterator"

	cases := []struct {
		name           string
		verbose        bool
		mockClient     kinesisiface.KinesisAPI
		expectAnyError bool
	}{
		{
			name:    "cannot get shard iterator",
			verbose: false,
			mockClient: &mockKinesis{
				getShardIteratorFunc: func(input *kinesis.GetShardIteratorInput) (*kinesis.GetShardIteratorOutput, error) {
					return nil, errors.New("intended unit test error")
				},
			},
			expectAnyError: true,
		},
		{
			name:    "cannot get records from shard",
			verbose: false,
			mockClient: &mockKinesis{
				getShardIteratorFunc: func(input *kinesis.GetShardIteratorInput) (*kinesis.GetShardIteratorOutput, error) {
					return &kinesis.GetShardIteratorOutput{
						ShardIterator: &fakeIterator,
					}, nil
				},
				getRecordsFunc: func(input *kinesis.GetRecordsInput) (*kinesis.GetRecordsOutput, error) {
					return nil, errors.New("intended unit test error")
				},
			},
			expectAnyError: true,
		},
	}

	for _, test := range cases {
		c := New()
		c.Verbose = test.verbose
		c.Client = test.mockClient
		c.readOneShard("fake-stream", &kinesis.Shard{
			ShardId:             &fakeShardID,
			SequenceNumberRange: &kinesis.SequenceNumberRange{},
		}, time.Now())
		err := <-c.errChan
		if test.expectAnyError {
			assert.Error(t, err, "test [%s] failed", test.name)
		} else {
			assert.NoError(t, err, "test [%s] failed", test.name)
		}
	}
}

func TestPrintRecords(t *testing.T) {
	fakeIterator := "fake-iterator"

	cases := []struct {
		name             string
		mockClient       kinesisiface.KinesisAPI
		mockIsAggregated func(*kinesis.Record) bool
		mockDeaggregate  func(*kinesis.Record) []*kinesis.Record
		expectAnyError   bool
		expectedOutput   string
	}{
		{
			name: "cannot get records",
			mockClient: &mockKinesis{
				getRecordsFunc: func(input *kinesis.GetRecordsInput) (*kinesis.GetRecordsOutput, error) {
					return nil, errors.New("intended unit test error")
				},
			},
			expectAnyError: true,
		},
		{
			name: "print aggregated records",
			mockClient: &mockKinesis{
				getRecordsFunc: func(input *kinesis.GetRecordsInput) (*kinesis.GetRecordsOutput, error) {
					return &kinesis.GetRecordsOutput{
						Records: []*kinesis.Record{
							&kinesis.Record{
								Data: []byte("aggregated"),
							},
						},
					}, nil
				},
			},
			mockIsAggregated: func(record *kinesis.Record) bool {
				return true
			},
			mockDeaggregate: func(record *kinesis.Record) []*kinesis.Record {
				return []*kinesis.Record{record}
			},
			expectAnyError: false,
			expectedOutput: "aggregated\n",
		},
		{
			name: "print plain text records",
			mockClient: &mockKinesis{
				getRecordsFunc: func(input *kinesis.GetRecordsInput) (*kinesis.GetRecordsOutput, error) {
					return &kinesis.GetRecordsOutput{
						Records: []*kinesis.Record{
							&kinesis.Record{
								Data: []byte("plain text"),
							},
						},
					}, nil
				},
			},
			mockIsAggregated: func(record *kinesis.Record) bool {
				return false
			},
			expectAnyError: false,
			expectedOutput: "plain text\n",
		},
	}

	for _, test := range cases {
		c := New()
		c.Client = test.mockClient
		var buf bytes.Buffer
		c.out = &buf
		oldIsAggregated := isAggregated
		oldDeaggregate := deaggregate
		oldPrintOneRecord := printOneRecord
		defer func() {
			isAggregated = oldIsAggregated
			deaggregate = oldDeaggregate
			printOneRecord = oldPrintOneRecord
		}()
		isAggregated = test.mockIsAggregated
		deaggregate = test.mockDeaggregate
		printOneRecord = func(out io.Writer, stream string, shard *kinesis.Shard, record *kinesis.Record, compress string, verbose bool) {
			fmt.Fprintln(out, string(record.Data))
		}
		_, err := c.printRecords(&fakeIterator, "fake-stream", new(kinesis.Shard))
		if test.expectAnyError {
			assert.Error(t, err, "test [%s] failed", test.name)
		} else {
			assert.NoError(t, err, "test [%s] failed", test.name)
			assert.Equal(t, test.expectedOutput, buf.String(), "test [%s] failed", test.name)
		}
	}
}

func TestIsAggregated(t *testing.T) {
	cases := []struct {
		name     string
		input    []byte
		expected bool
	}{
		{
			name:     "not aggregated",
			input:    []byte("not aggregated"),
			expected: false,
		},
		{
			name:     "aggregated",
			input:    append(magicNumber, []byte("aggregated")...),
			expected: true,
		},
	}

	for _, test := range cases {
		actual := isAggregated(&kinesis.Record{
			Data: test.input,
		})
		assert.Equal(t, test.expected, actual, "test [%s] failed", test.name)
	}
}

func TestDeaggregate(t *testing.T) {
	key1 := "key1"
	key2 := "key2"
	input := []*kinesis.Record{
		&kinesis.Record{
			PartitionKey: &key1,
			Data:         []byte("record1"),
		},
		&kinesis.Record{
			PartitionKey: &key2,
			Data:         []byte("record2"),
		},
	}
	record, err := createAggregateRecord(input)
	require.NoError(t, err)

	deaggregated := deaggregate(record)

	assert.Equal(t, input, deaggregated)
}

func createAggregateRecord(input []*kinesis.Record) (*kinesis.Record, error) {
	var (
		keys    []string
		records []*aggregated.Record
	)
	for _, r := range input {
		keys = append(keys, *r.PartitionKey)
		keyIndex := uint64(len(keys) - 1)
		records = append(records, &aggregated.Record{
			Data:              r.Data,
			PartitionKeyIndex: &keyIndex,
		})

	}
	data, err := proto.Marshal(&aggregated.AggregatedRecord{
		PartitionKeyTable: keys,
		Records:           records,
	})
	if err != nil {
		return nil, err
	}

	md5Hash := md5.New()
	md5Hash.Write(data)
	checkSum := md5Hash.Sum(nil)
	data = append(magicNumber, data...)
	data = append(data, checkSum...)

	return &kinesis.Record{
		Data: data,
	}, nil
}

func TestPrintOneRecord(t *testing.T) {
	fakeTimestamp := time.Now()
	fakeData := []byte("something cool")
	fakeShardID := "fake-shard-id"
	fakeSequenceNumber := "fake-sequence-number"
	cases := []struct {
		name     string
		verbose  bool
		stream   string
		shard    *kinesis.Shard
		record   *kinesis.Record
		expected string
		compress string
	}{
		{
			name:    "non verbose mode",
			verbose: false,
			record: &kinesis.Record{
				ApproximateArrivalTimestamp: &fakeTimestamp,
				Data:                        fakeData,
			},
			expected: fmt.Sprintf(
				"%s %s\n",
				fakeTimestamp.Format(recordDatetimeFormat),
				string(fakeData),
			),
		},
		{
			name:    "verbose mode",
			verbose: true,
			stream:  "fake-stream",
			shard: &kinesis.Shard{
				ShardId: &fakeShardID,
			},
			record: &kinesis.Record{
				ApproximateArrivalTimestamp: &fakeTimestamp,
				SequenceNumber:              &fakeSequenceNumber,
				Data:                        fakeData,
			},
			expected: fmt.Sprintf(
				"%s %s %s %s %s\n",
				fakeTimestamp.Format(recordDatetimeFormat),
				"fake-stream",
				fakeShardID,
				fakeSequenceNumber,
				string(fakeData),
			),
		},
	}

	for _, test := range cases {
		var buf bytes.Buffer
		printOneRecord(&buf, test.stream, test.shard, test.record, test.compress, test.verbose)
		actual := buf.String()
		assert.Equal(t, test.expected, actual, "test [%s] failed", test.name)
	}
}
