package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	awsLambda "github.com/aws/aws-sdk-go-v2/service/lambda"
	"github.com/novalagung/gubrak/v2"
)

type CountItemKey struct {
	PK string `json:"pk" dynamodbav:"pk"`
	SK string `json:"sk" dynamodbav:"sk"`
}

type CountItem struct {
	CountItemKey
	Count int      `json:"count" dynamodbav:"count"`
	Ids   []string `json:"ids" dynamodbav"ids"`
}

type FulfillerPayload struct {
	Shard int      `json:"shard"`
	Ids   []string `json:"ids" dynamodbav"ids"`
}

func handler(ctx context.Context) error {
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		fmt.Println("could not load the config")
		return err
	}

	db := dynamodb.NewFromConfig(cfg)

	firstShardKey, err := getKeyForShard(1)
	if err != nil {
		fmt.Println("Could not create keys for first shard")
		return err
	}

	secondShardKey, err := getKeyForShard(2)
	if err != nil {
		fmt.Println("Could not create keys for second shard")
		return err
	}

	out, err := db.BatchGetItem(ctx, &dynamodb.BatchGetItemInput{
		RequestItems: map[string]types.KeysAndAttributes{
			os.Getenv("TABLE_NAME"): {
				Keys: []map[string]types.AttributeValue{
					firstShardKey,
					secondShardKey,
				},
			},
		},
	})
	if err != nil || len(out.UnprocessedKeys) != 0 {
		fmt.Println("Batch get failed", err)
		return err
	}

	items, found := out.Responses[os.Getenv("TABLE_NAME")]
	if !found {
		fmt.Println("Respoinsed not found for the table")
		return nil
	}

	fmt.Println("COUNT ITEMS RECOVERED", items)

	countItems := make([]CountItem, 2)
	for i, item := range items {
		var countItem CountItem
		err := attributevalue.UnmarshalMap(item, &countItem)
		if err != nil {
			fmt.Println("Could not unmarshal item", err)
			return nil
		}

		countItems[i] = countItem
	}

	firstShardChunks := [][]string{}
	secondShardChunks := [][]string{}

	for _, countItem := range countItems {
		shard := getShardFromCountItem(countItem)
		maybeChunks := gubrak.From(countItem.Ids).Chunk(5).Result()
		if maybeChunks == nil {
			fmt.Println("Nothing to chunk, returning")
			return nil
		}

		chunks := maybeChunks.([][]string)

		if shard == 1 {
			firstShardChunks = append(firstShardChunks, chunks...)
		}
		if shard == 2 {
			secondShardChunks = append(secondShardChunks, chunks...)
		}
	}

	lambdaService := awsLambda.NewFromConfig(cfg)
	for _, firstShardChunk := range firstShardChunks {
		buf, err := json.Marshal(FulfillerPayload{Shard: 1, Ids: firstShardChunk})
		if err != nil {
			fmt.Println("Could not marshal first shard chunk", err)
			return err
		}

		out, err := lambdaService.InvokeAsync(ctx, &awsLambda.InvokeAsyncInput{
			FunctionName: aws.String(os.Getenv("FULFILLER_NAME")),
			InvokeArgs:   bytes.NewReader(buf),
		})
		fmt.Println("ASync invocation status code:", out.Status)
		if err != nil {
			fmt.Println("Could not invoke the function")
			return err
		}
	}

	for _, secondChardChunk := range secondShardChunks {
		buf, err := json.Marshal(FulfillerPayload{Shard: 2, Ids: secondChardChunk})
		if err != nil {
			fmt.Println("Could not marshal second shard chunk", err)
			return err
		}

		out, err := lambdaService.InvokeAsync(ctx, &awsLambda.InvokeAsyncInput{
			FunctionName: aws.String(os.Getenv("FULFILLER_NAME")),
			InvokeArgs:   bytes.NewReader(buf),
		})
		fmt.Println("ASync invocation status code:", out.Status)
		if err != nil {
			fmt.Println("Could not invoke the function")
			return err
		}
	}

	fmt.Println("First count chunks", firstShardChunks)
	fmt.Println("Second count chunks", secondShardChunks)

	return nil
}

func getKeyForShard(shard int) (map[string]types.AttributeValue, error) {
	key := CountItemKey{
		PK: fmt.Sprintf("COUNT#%v", shard),
		SK: fmt.Sprintf("COUNT#%v", shard),
	}

	keyMap, err := attributevalue.MarshalMap(key)
	return keyMap, err
}

func getShardFromCountItem(ci CountItem) int {
	shardStr := strings.Replace(ci.SK, "COUNT#", "", -1)
	if shardStr == "1" {
		return 1
	}

	return 2
}

func main() {
	lambda.Start(handler)
}
