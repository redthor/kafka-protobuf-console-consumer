package consumer

import (
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/DNAlchemist/kafka-protobuf-console-consumer/protobuf_decoder"
)

type SimpleConsumerGroupHandler struct {
	protobufJSONStringify *protobuf_decoder.ProtobufJSONStringify
	prettyJson            bool
	fromBeginning         bool
	withSeparator         bool
	messageInfo           bool
}

func NewSimpleConsumerGroupHandler(protobufJSONStringify *protobuf_decoder.ProtobufJSONStringify, prettyJson bool, fromBeginning bool, withSeparator bool, messageInfo bool) *SimpleConsumerGroupHandler {
	return &SimpleConsumerGroupHandler{protobufJSONStringify: protobufJSONStringify, prettyJson: prettyJson, fromBeginning: fromBeginning, withSeparator: withSeparator, messageInfo: messageInfo}
}

func (SimpleConsumerGroupHandler) Setup(_ sarama.ConsumerGroupSession) error   { return nil }
func (SimpleConsumerGroupHandler) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }
func (h SimpleConsumerGroupHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		if h.messageInfo {
			fmt.Printf("Message key:%q topic:%q partition:%d offset:%d\n", msg.Key, msg.Topic, msg.Partition, msg.Offset)
		}
		jsonString, e := h.protobufJSONStringify.JsonString(msg.Value, h.prettyJson)
		if e != nil {
			fmt.Println(e)
		}
		fmt.Println(jsonString)
		if h.withSeparator {
			fmt.Println("--------------------------------- end message -----------------------------------------")
		}
		sess.MarkMessage(msg, "")
	}
	return nil
}

