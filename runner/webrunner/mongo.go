package webrunner

import (
	"context"
	"log"
	"time"

	"github.com/spf13/cast"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

type UpdateInput struct {
	ID           string
	NewRating    string
	NewReviewCnt string
}

func UpdateRatingsAndReviews(ctx context.Context, input UpdateInput, mongoDb mongo.Database) error {
	filter := bson.M{"_id": input.ID}

	update := bson.M{
		"$set": bson.M{
			"ratings":     cast.ToFloat64(input.NewRating),
			"noOfReviews": cast.ToInt64(input.NewReviewCnt),
			"updatedAt":   time.Now(),
		},
	}

	result, err := mongoDb.Collection("facilities").UpdateOne(ctx, filter, update)
	log.Println("Update result:", result)
	return err
}
