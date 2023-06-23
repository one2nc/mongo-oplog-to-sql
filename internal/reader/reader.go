package reader

import (
	"context"

	"github.com/one2nc/mongo-oplog-to-sql/internal/domain"
)

// OplogReader defines the interface for reading Oplog entries and storing them in the publisher.
type OplogReader interface {
	ReadOplogs(ctx context.Context, publisher domain.OplogPublisher) error
}
