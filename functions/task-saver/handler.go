package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/oklog/ulid/v2"
)

type Payload struct {
	Task string `json:"task"`
}

type TaskItem struct {
	PK   string `json:"pk" dynamodbav:"pk"`
	SK   string `json:"sk" dynamodbav:"sk"`
	Task string `json:"task" dynamodbav:"task"`
}

type CountItem struct {
	PK string `json:"pk" dynamodbav:"pk"`
	SK string `json:"sk" dynamodbav:"sk"`
}

func handler(ctx context.Context, request events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return respond(http.StatusInternalServerError, "Config could not be loaded"), err
	}

	db := dynamodb.NewFromConfig(cfg)

	var payload Payload
	err = json.Unmarshal([]byte(request.Body), &payload)
	if err != nil {
		return respond(http.StatusBadRequest, "No body found"), nil
	}

	taskID := getID()
	shard := getShard()

	taskItem := TaskItem{
		PK:   fmt.Sprintf("TASK#%v", shard),
		SK:   fmt.Sprintf("TASK#%v", taskID),
		Task: payload.Task,
	}

	taskItemPut, err := getPutTaskType(taskItem)
	if err != nil {
		return respond(http.StatusInternalServerError, "Could not create task item put"), nil
	}

	countItemUpdate, err := getCountUpdateType(shard, taskID)
	if err != nil {
		return respond(http.StatusInternalServerError, "Could not create count item update"), nil
	}

	_, err = db.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
		TransactItems: []types.TransactWriteItem{
			{
				Put: taskItemPut,
			},
			{
				Update: countItemUpdate,
			},
		},
	})
	if err != nil {
		return respond(http.StatusInternalServerError, fmt.Sprintf("Could not write %v", err)), nil
	}

	return respond(http.StatusOK, "Event put"), nil
}

func getPutTaskType(taskItem TaskItem) (*types.Put, error) {
	itemMap, err := attributevalue.MarshalMap(taskItem)
	if err != nil {
		return nil, err
	}

	return &types.Put{
		Item:      itemMap,
		TableName: aws.String(os.Getenv("TABLE_NAME")),
	}, nil
}

func getCountUpdateType(shard int, taskID string) (*types.Update, error) {
	countItem := CountItem{
		PK: fmt.Sprintf("COUNT#%v", shard),
		SK: fmt.Sprintf("COUNT#%v", shard),
	}
	countItemMap, err := attributevalue.MarshalMap(countItem)
	if err != nil {
		return nil, err
	}

	return &types.Update{
		Key:              countItemMap,
		TableName:        aws.String(os.Getenv("TABLE_NAME")),
		UpdateExpression: aws.String("ADD #count :one, #ids :ids"),
		ExpressionAttributeNames: map[string]string{
			"#count": "count",
			"#ids":   "ids",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":one": &types.AttributeValueMemberN{
				Value: "1",
			},
			":ids": &types.AttributeValueMemberSS{
				Value: []string{taskID},
			},
		},
	}, nil
}

func getID() string {
	t := time.Now()
	entropy := ulid.Monotonic(rand.New(rand.NewSource(t.UnixNano())), 0)
	taskID := ulid.MustNew(ulid.Timestamp(t), entropy)

	return taskID.String()
}

func getShard() int {
	rand.Seed(time.Now().Unix())
	shard := rand.Intn(2) + 1

	return shard
}

func respond(status int, body string) events.APIGatewayProxyResponse {
	return events.APIGatewayProxyResponse{
		StatusCode: status,
		Body:       body,
	}
}

func main() {
	lambda.Start(handler)
}
