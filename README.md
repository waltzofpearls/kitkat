# kitkat

<img align="left" alt="kitkat-logo" src="https://user-images.githubusercontent.com/965430/48188837-84ef3680-e2f3-11e8-9ccb-006d317be756.png">

Kitkat is a command line Kinesis Streams producer and consumer.

In producer mode kitkat reads messages from stdin, delimited with new line (\n), and produces them
to the provided Kinesis Streams (--stream or -t)

In consumer mode kitkat reads messages from Kinesis Streams shards and prints them to stdout.

You will be able to produce and consume messages in KPL aggregated record format. This feature is
currently under development.

## Getting started

To install kitkat locally, make sure you have Go 1.10+ on your computer, and then run

```shell
go get -u github.com/waltzofpearls/kitkat
```

To see the usage of kitkat, run

```
$> kitkat --help

# Producer usage
$> kitkat p --help
$> kitkat produce --help

# Consumer usage
$> kitkat c --help
$> kitkat consume --help
```

## Examples

Read messages from stdin, produce to `my-demo-stream`

```shell
$> kitkat p -t my-demo-stream

# Use a non default AWS profile name
$> AWS_PROFILE=my-profile kitkat p -t my-demo-stream

# Enable verbose mode, which prints shard ID and sequence number
# from the newly produced message
$> AWS_PROFILE=my-profile kitkat p -t my-demo-stream -v

# Tail syslog and pipe to kitkat producer
$> tail /var/log/syslog | AWS_PROFILE=my-profile kitkat p -t my-demo-stream -v
```

Consume and tail messages from `my-demo-stream`

```
$> kitkat c -t my-demo-stream

# Use a non default AWS profile name
$> AWS_PROFILE=my-profile kitkat c -t my-demo-stream

# Enable verbose mode, which prints stream info, message shard ID
# and message sequence number
$> AWS_PROFILE=my-profile kitkat c -t my-demo-stream -v
```

<p align="center"><strong>Enjoy!</strong></p>
<p align="center">
  <img alt="kitkat-animated" src="https://user-images.githubusercontent.com/965430/48188771-638e4a80-e2f3-11e8-8a9b-122b1db424c4.gif">
</p>
