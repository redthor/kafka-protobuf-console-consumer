package consumer

import (
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/DNAlchemist/kafka-protobuf-console-consumer/protobuf_decoder"
	"strings"
)

type SimpleConsumerGroupHandler struct {
	protobufJSONStringify *protobuf_decoder.ProtobufJSONStringify
	prettyJson            bool
	fromBeginning         bool
	withSeparator         bool
	messageInfo           bool
	headers               bool
}

func NewSimpleConsumerGroupHandler(protobufJSONStringify *protobuf_decoder.ProtobufJSONStringify, prettyJson bool, fromBeginning bool, withSeparator bool, messageInfo bool, headers bool) *SimpleConsumerGroupHandler {
	return &SimpleConsumerGroupHandler{protobufJSONStringify: protobufJSONStringify, prettyJson: prettyJson, fromBeginning: fromBeginning, withSeparator: withSeparator, messageInfo: messageInfo, headers: headers}
}

func (SimpleConsumerGroupHandler) Setup(_ sarama.ConsumerGroupSession) error   { return nil }
func (SimpleConsumerGroupHandler) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }
func (h SimpleConsumerGroupHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {

		var message strings.Builder

		if h.messageInfo {
			message.WriteString(fmt.Sprintf("Message key:%q topic:%q partition:%d offset:%d\n", msg.Key, msg.Topic, msg.Partition, msg.Offset))
		}

		if h.headers {
			message.WriteString("{\"headers\":{")
			var first = true
			for _, h := range msg.Headers {
				if first {
					first = false
				} else {
					message.WriteString(",")
				}
				message.WriteString(fmt.Sprintf("\"%s\": \"%s\"", h.Key, h.Value))
			}
			message.WriteString("},")
		}

		jsonString, e := h.protobufJSONStringify.JsonString(msg.Value, h.prettyJson)
		if e != nil {
			fmt.Println(e)
		}

		if h.headers {
			message.WriteString("\"message\":")
		}
		message.WriteString(jsonString)
		if h.headers {
			message.WriteString("}")
		}

		message.WriteString("\n")


		if h.withSeparator {
			message.WriteString("--------------------------------- end message -----------------------------------------\n")
		}
		fmt.Print(message.String())
		sess.MarkMessage(msg, "")
	}
	return nil
}

