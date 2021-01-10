package main

import (
	"context"
	"fmt"
	"os"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type Payload struct {
	Shard int      `json:"shard"`
	Ids   []string `json:"ids"`
}

type DDBItemKey struct {
	PK string `json:"pk" dynamodbav:"pk"`
	SK string `json:"sk" dynamodbav:"sk"`
}

func handler(ctx context.Context, payload Payload) error {
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		fmt.Println("Count not load the config")
		return err
	}

	db := dynamodb.NewFromConfig(cfg)

	deleteRequests := make([]types.WriteRequest, len(payload.Ids))
	for i, id := range payload.Ids {
		key, err := attributevalue.MarshalMap(DDBItemKey{
			PK: fmt.Sprintf("TASK#%v", payload.Shard),
			SK: fmt.Sprintf("TASK#%v", id),
		})
		if err != nil {
			fmt.Println("Could not marshal stuff before forming delete request", err)
			return err
		}
		deleteRequests[i] = types.WriteRequest{DeleteRequest: &types.DeleteRequest{Key: key}}
	}

	_, err = db.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]types.WriteRequest{
			os.Getenv("TABLE_NAME"): deleteRequests,
		},
	})
	if err != nil {
		fmt.Println("Batch delete failed", err)
	}

	countKey, err := attributevalue.MarshalMap(DDBItemKey{
		PK: fmt.Sprintf("COUNT#%v", payload.Shard),
		SK: fmt.Sprintf("COUNT#%v", payload.Shard),
	})
	if err != nil {
		fmt.Println("Failed to create countKey", err)
	}

	_, err = db.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:        aws.String(os.Getenv("TABLE_NAME")),
		Key:              countKey,
		UpdateExpression: aws.String("ADD #count :count DELETE #ids :ids"),
		ExpressionAttributeNames: map[string]string{
			"#count": "count",
			"#ids":   "ids",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":ids":   &types.AttributeValueMemberSS{Value: payload.Ids},
			":count": &types.AttributeValueMemberN{Value: fmt.Sprintf("-%v", len(payload.Ids))},
		},
	})
	if err != nil {
		fmt.Println("Failed to update the count for shard", payload.Shard, err)
	}

	return nil
}

func main() {
	lambda.Start(handler)
}
