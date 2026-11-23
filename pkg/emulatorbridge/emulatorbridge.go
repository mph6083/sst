// Package emulatorbridge is the live function transport for AWS Emulator.
//
// The emulator runs function code in a sandbox with no network and no native binaries, so the
// regular bridge stub (a Go binary that dials AppSync Events) cannot run there. The emulator stub
// (platform/functions/emulator-bridge) can still call emulated AWS services, so packets travel
// stub -> CLI through an SQS FIFO queue and CLI -> stub through a DynamoDB table.
package emulatorbridge

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

// Packet types, mirroring bridge.MessageType. Declared here because the bridge package imports
// nothing from transports.
const (
	messagePing     = 1
	messageResponse = 3
	messageError    = 4
)

const (
	// maxNameLength keeps the queue name, with its .fifo suffix, inside the 80 character SQS limit.
	maxNameLength   = 75
	nameHashLength  = 8
	receiveWaitSecs = 10
	receiveBatch    = 10
	// packetRetention is how long response packets stay in the table before DynamoDB TTL removes them.
	packetRetention = time.Hour
	tableWaitLimit  = 30 * time.Second
)

var unsafeNameChars = regexp.MustCompile(`[^a-zA-Z0-9_-]`)

// Names of the queue and table one app stage relays through.
type Names struct {
	Queue string `json:"queue"`
	Table string `json:"table"`
}

// ResolveNames returns the deterministic queue and table names for an app stage.
func ResolveNames(app string, stage string) Names {
	base := unsafeNameChars.ReplaceAllString(fmt.Sprintf("sst-live-%s-%s", app, stage), "-")
	if len(base) > maxNameLength {
		sum := sha256.Sum256([]byte(base))
		base = base[:maxNameLength-nameHashLength-1] + "-" + hex.EncodeToString(sum[:])[:nameHashLength]
	}
	return Names{Queue: base + ".fifo", Table: base}
}

// Transport implements bridge.Transport on emulated SQS and DynamoDB.
type Transport struct {
	sqs      *sqs.Client
	dynamo   *dynamodb.Client
	names    Names
	queueURL string
}

// Ensure creates the queue and table when they do not exist yet. It is safe to call repeatedly.
func Ensure(ctx context.Context, cfg aws.Config, names Names) (*Transport, error) {
	transport := &Transport{
		sqs:    sqs.NewFromConfig(cfg),
		dynamo: dynamodb.NewFromConfig(cfg),
		names:  names,
	}

	queue, err := transport.sqs.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String(names.Queue),
		Attributes: map[string]string{
			"FifoQueue":         "true",
			"VisibilityTimeout": "30",
		},
	})
	if err != nil {
		return nil, fmt.Errorf("creating live queue %s: %w", names.Queue, err)
	}
	transport.queueURL = *queue.QueueUrl

	_, err = transport.dynamo.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName:   aws.String(names.Table),
		BillingMode: ddbtypes.BillingModePayPerRequest,
		AttributeDefinitions: []ddbtypes.AttributeDefinition{
			{AttributeName: aws.String("id"), AttributeType: ddbtypes.ScalarAttributeTypeS},
			{AttributeName: aws.String("idx"), AttributeType: ddbtypes.ScalarAttributeTypeN},
		},
		KeySchema: []ddbtypes.KeySchemaElement{
			{AttributeName: aws.String("id"), KeyType: ddbtypes.KeyTypeHash},
			{AttributeName: aws.String("idx"), KeyType: ddbtypes.KeyTypeRange},
		},
	})
	var inUse *ddbtypes.ResourceInUseException
	if err != nil && !errors.As(err, &inUse) {
		return nil, fmt.Errorf("creating live table %s: %w", names.Table, err)
	}
	created := err == nil

	deadline := time.Now().Add(tableWaitLimit)
	for {
		described, err := transport.dynamo.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: aws.String(names.Table)})
		if err == nil && described.Table.TableStatus == ddbtypes.TableStatusActive {
			break
		}
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("live table %s did not become active", names.Table)
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(200 * time.Millisecond):
		}
	}

	if created {
		_, err = transport.dynamo.UpdateTimeToLive(ctx, &dynamodb.UpdateTimeToLiveInput{
			TableName: aws.String(names.Table),
			TimeToLiveSpecification: &ddbtypes.TimeToLiveSpecification{
				AttributeName: aws.String("expiresAt"),
				Enabled:       aws.Bool(true),
			},
		})
		if err != nil {
			slog.Warn("could not enable ttl on live table", "table", names.Table, "err", err)
		}
	}
	return transport, nil
}

// Purge drops invocations left in the queue by an earlier dev session, so they are not replayed.
func (t *Transport) Purge(ctx context.Context) {
	_, err := t.sqs.PurgeQueue(ctx, &sqs.PurgeQueueInput{QueueUrl: aws.String(t.queueURL)})
	if err != nil {
		slog.Warn("could not purge live queue", "err", err)
	}
}

// Subscribe long-polls the queue and returns every packet the stubs send. The channel name is
// unused: the queue belongs to one app stage.
func (t *Transport) Subscribe(ctx context.Context, channel string) (chan string, error) {
	out := make(chan string, 1000)
	go func() {
		for {
			if ctx.Err() != nil {
				return
			}
			received, err := t.sqs.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
				QueueUrl:            aws.String(t.queueURL),
				MaxNumberOfMessages: receiveBatch,
				WaitTimeSeconds:     receiveWaitSecs,
			})
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				slog.Error("live queue receive failed", "err", err)
				select {
				case <-ctx.Done():
					return
				case <-time.After(time.Second):
				}
				continue
			}
			if len(received.Messages) == 0 {
				continue
			}
			entries := []sqstypes.DeleteMessageBatchRequestEntry{}
			for index, message := range received.Messages {
				out <- aws.ToString(message.Body)
				entries = append(entries, sqstypes.DeleteMessageBatchRequestEntry{
					Id:            aws.String(strconv.Itoa(index)),
					ReceiptHandle: message.ReceiptHandle,
				})
			}
			_, err = t.sqs.DeleteMessageBatch(ctx, &sqs.DeleteMessageBatchInput{
				QueueUrl: aws.String(t.queueURL),
				Entries:  entries,
			})
			if err != nil {
				slog.Error("live queue delete failed", "err", err)
			}
		}
	}()
	return out, nil
}

type packet struct {
	Type   int    `json:"type"`
	Source string `json:"source"`
	ID     string `json:"id"`
	Index  int    `json:"index"`
	Data   string `json:"data"`
	Final  bool   `json:"final"`
}

// Publish stores a packet for a stub. Responses and errors are keyed by the request id the stub is
// polling for. A ping is one row per worker that says the CLI is alive. Other packet types have no
// meaning for the emulator stub, which sends its init packet with every invocation.
func (t *Transport) Publish(ctx context.Context, channel string, event interface{}) error {
	encoded, err := json.Marshal(event)
	if err != nil {
		return err
	}
	var decoded packet
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		return err
	}
	expiresAt := strconv.FormatInt(time.Now().Add(packetRetention).Unix(), 10)

	if decoded.Type == messagePing {
		_, err := t.dynamo.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: aws.String(t.names.Table),
			Item: map[string]ddbtypes.AttributeValue{
				"id":        &ddbtypes.AttributeValueMemberS{Value: "ping:" + workerFromChannel(channel)},
				"idx":       &ddbtypes.AttributeValueMemberN{Value: "0"},
				"at":        &ddbtypes.AttributeValueMemberN{Value: strconv.FormatInt(time.Now().UnixMilli(), 10)},
				"expiresAt": &ddbtypes.AttributeValueMemberN{Value: expiresAt},
			},
		})
		return err
	}
	if decoded.Type != messageResponse && decoded.Type != messageError {
		return nil
	}
	_, err = t.dynamo.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(t.names.Table),
		Item: map[string]ddbtypes.AttributeValue{
			"id":        &ddbtypes.AttributeValueMemberS{Value: "response:" + decoded.ID},
			"idx":       &ddbtypes.AttributeValueMemberN{Value: strconv.Itoa(decoded.Index)},
			"type":      &ddbtypes.AttributeValueMemberN{Value: strconv.Itoa(decoded.Type)},
			"data":      &ddbtypes.AttributeValueMemberS{Value: decoded.Data},
			"final":     &ddbtypes.AttributeValueMemberBOOL{Value: decoded.Final},
			"expiresAt": &ddbtypes.AttributeValueMemberN{Value: expiresAt},
		},
	})
	return err
}

// workerFromChannel extracts the worker id from a "<prefix>/<workerID>/in" channel.
func workerFromChannel(channel string) string {
	parts := strings.Split(strings.TrimSuffix(channel, "/in"), "/")
	return parts[len(parts)-1]
}
