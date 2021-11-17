package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/DNAlchemist/kafka-protobuf-console-consumer/consumer"
	"github.com/DNAlchemist/kafka-protobuf-console-consumer/protobuf_decoder"
	. "github.com/Shopify/sarama"
	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	version                  = kingpin.Flag("version", "Version").Short('v').Bool()
	debug                    = kingpin.Flag("debug", "Enable Sarama logs").Short('d').Bool()
	brokerList               = kingpin.Flag("broker-list", "List of brokers to connect").Short('b').Default("localhost:9092").Strings()
	consumerGroupName        = kingpin.Flag("consumer-group", "Consumer group to use").Short('c').String()
	topic                    = kingpin.Flag("topic", "Topic name").Short('t').String()
	protoImportDirs          = kingpin.Flag("proto-dir", "/foo/dir1 /bar/dir2 (add all dirs used by imports)").Strings()
	protoFileNameWithMessage = kingpin.Flag("file", "will be baz/a.proto that's in /foo/dir1/baz/a.proto").String()
	messageName              = kingpin.Flag("message", "Proto message name").String()
	headers                  = kingpin.Flag("headers", "Print headers").Bool()

	fromBeginning 			 = kingpin.Flag("from-beginning", "Read from beginning").Bool()
	prettyJson    			 = kingpin.Flag("pretty", "Format output").Bool()
	messageInfo    			 = kingpin.Flag("message-info", "Print key, topic, partition, offset").Bool()
	withSeparator 			 = kingpin.Flag("with-separator", "Adds separator between messages. Useful with --pretty").Bool()

	// make will provide the version details for the release executable
	versionInfo string
	versionDate string
)

func main() {
	kingpin.Parse()

	if *version {
		fmt.Printf("%s - %s", versionInfo, versionDate)
		os.Exit(0)
	}

	if len(*brokerList) == 0 || len(*topic) == 0 || len(*protoFileNameWithMessage) == 0 ||
		len(*messageName) == 0 {
		// TODO fix --help should work when Flags are marked Required, currently its supported by making Flags optional and checking this way
		fmt.Println("Missing required params; try --help")
		os.Exit(1)
	}
	// Start a new consumer group
	consumerGroup := consumerGroup()
	if *messageInfo {
		fmt.Printf("Starting %s build-on %s with consumer group: %s\n\n", versionInfo, versionDate, consumerGroup)
	}

	// Init config, specify appropriate version
	config := NewConfig()
	config.Version = V2_2_0_0
	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.Initial = offset()
	config.Net.TLS.Enable = true
	if *debug {
		Logger = log.New(os.Stdout, "[sarama] ", log.LstdFlags)
	}

	// Start with a client
	client, err := NewClient(*brokerList, config)
	if err != nil {
		panic(err)
	}
	defer func() { _ = client.Close() }()

	group, err := NewConsumerGroupFromClient(consumerGroup, client)
	if err != nil {
		panic(err)
	}
	defer func() { _ = group.Close() }()

	// Track errors
	go func() {
		for err := range group.Errors() {
			fmt.Println("group error", err)
		}
	}()

	// Iterate over consumer sessions.
	ctx := context.Background()
	for {
		topics := []string{*topic}
		protobufJSONStringify, err := protobuf_decoder.NewProtobufJSONStringify(*protoImportDirs, *protoFileNameWithMessage, *messageName)
		if err != nil {
			panic(err)
		}

		handler := consumer.NewSimpleConsumerGroupHandler(
			protobufJSONStringify, *prettyJson, *fromBeginning, *withSeparator, *messageInfo, *headers)

		err = group.Consume(ctx, topics, handler)
		if err != nil {
			panic(err)
		}
	}
}

func consumerGroup() string {
	if len(*consumerGroupName) > 0 {
		return *consumerGroupName
	}
	return fmt.Sprintf("kafka-protobuf-console-consumer-%d", time.Now().UnixNano()/1000000)
}

func offset() int64 {
	if *fromBeginning {
		return OffsetOldest
	}
	return OffsetNewest
}