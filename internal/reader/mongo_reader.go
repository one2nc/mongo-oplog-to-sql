package reader

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/one2nc/mongo-oplog-to-sql/internal/domain"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	MONGO_DB_NAME    string = "local"
	MONGO_COLLECTION string = "oplog.rs"
)

// MongoReader implements the OplogReader interface for reading Oplog entries from a running MongoDB instance.
type MongoReader struct {
	ConnectionString string
}

// NewMongoReader creates a new instance of FileReader.
func NewMongoReader(connectionStr string) OplogReader {
	return &MongoReader{
		ConnectionString: connectionStr,
	}
}

// ReadOplogs reads Oplog entries from the file and publish them in the publisher.
func (mr *MongoReader) ReadOplogs(ctx context.Context, publisher domain.OplogPublisher) error {
	defer publisher.Stop()

	// Create a MongoDB client
	client, err := mr.getMongoClient()
	if err != nil {
		return err
	}
	defer client.Disconnect(ctx)

	oplogCollection := client.Database(MONGO_DB_NAME).Collection(MONGO_COLLECTION)

	findOptions := options.Find().SetCursorType(options.TailableAwait)
	cursor, err := oplogCollection.Find(context.Background(), userFilter(), findOptions)
	if err != nil {
		panic(err)
	}
	defer cursor.Close(ctx)

	for {
		// Check if the context is done
		select {
		case <-ctx.Done():
			// The context is done, stop reading Oplogs
			return nil
		default:
			// Context is still active, continue reading Oplogs
		}

		if cursor.TryNext(context.TODO()) {
			var data bson.M
			if err := cursor.Decode(&data); err != nil {
				panic(err)
			}

			jsonData, err := json.Marshal(data)
			if err != nil {
				panic(err)
			}

			var entry domain.OplogEntry
			err = json.Unmarshal(jsonData, &entry)
			if err != nil {
				panic(err)
			}

			err = publisher.PublishOplog(entry)
			if err != nil {
				return err
			}
		}

		if err := cursor.Err(); err != nil {
			panic(err)
		}

		if cursor.ID() == 0 {
			break
		}
	}
	return nil
}

func (mr *MongoReader) getMongoClient() (*mongo.Client, error) {
	// Connect to the MongoDB server
	clientOptions := options.Client().ApplyURI(mr.ConnectionString).SetDirect(true)
	client, err := mongo.Connect(context.Background(), clientOptions)
	if err != nil {
		fmt.Println("Error connecting to MongoDB:", err)
		return nil, err
	}
	return client, nil
}

func userFilter() primitive.M {
	filter := bson.M{
		"op": bson.M{"$nin": []string{"n", "c"}},
		"$and": []bson.M{
			{"ns": bson.M{"$not": bson.M{"$regex": "^(admin|config)\\."}}},
			{"ns": bson.M{"$not": bson.M{"$eq": ""}}},
		},
	}
	return filter
}
