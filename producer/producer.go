package producer

import (
	"bufio"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
)

type Producer struct {
	Stream       string
	Region       string
	Aggregated   bool
	Verbose      bool
	PartitionKey string

	client *kinesis.Kinesis
}

func New() *Producer {
	return new(Producer)
}

func (p *Producer) Write() error {
	p.client = kinesis.New(
		session.New(&aws.Config{
			Region: aws.String(p.Region),
		}),
	)
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		p.write(scanner.Text())
	}
	return scanner.Err()
}

func (p *Producer) write(message string) {
	out, err := p.client.PutRecord(&kinesis.PutRecordInput{
		Data:         []byte(message),
		StreamName:   aws.String(p.Stream),
		PartitionKey: aws.String(p.partitionKey()),
	})
	if err != nil {
		fmt.Println("ERROR:", err)
	}
	if p.Verbose {
		fmt.Println(*out.ShardId, *out.SequenceNumber)
		fmt.Println("---")
	}

}

func (p *Producer) partitionKey() string {
	if p.PartitionKey != "" {
		return p.PartitionKey
	}
	key, err := randomHex(16)
	if err != nil {
		return "deadbeef"
	}
	return key
}

func randomHex(n int) (string, error) {
	bytes := make([]byte, n)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	return hex.EncodeToString(bytes), nil
}
