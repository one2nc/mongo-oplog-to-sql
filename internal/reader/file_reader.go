package reader

import (
	"context"
	"encoding/json"
	"log"
	"os"

	"github.com/one2nc/mongo-oplog-to-sql/internal/domain"
)

// FileReader implements the OplogReader interface for reading Oplog entries from a file.
type FileReader struct {
	FilePath string
}

// NewFileReader creates a new instance of FileReader.
func NewFileReader(filePath string) OplogReader {
	return &FileReader{
		FilePath: filePath,
	}
}

// ReadOplogs reads Oplog entries from the file and publish them in the publisher.
func (fr *FileReader) ReadOplogs(ctx context.Context, publisher domain.OplogPublisher) error {
	defer publisher.Stop()

	oplogFile, err := os.Open(fr.FilePath)
	if err != nil {
		log.Fatalf("file could not be opened : %s", err)
		return err
	}
	defer oplogFile.Close()

	decoder := json.NewDecoder(oplogFile)
	if _, err := decoder.Token(); err != nil {
		log.Fatal("invalid file")
		return err
	}

	i := 1
	for decoder.More() {
		// Check if the context is done
		select {
		case <-ctx.Done():
			// The context is done, stop reading Oplogs
			return nil
		default:
			// Context is still active, continue reading Oplogs
		}

		var entry domain.OplogEntry
		if err := decoder.Decode(&entry); err != nil {
			log.Fatalf("invalid json field on line %d:%s", i, err)
			return err
		}
		err := publisher.PublishOplog(entry)
		if err != nil {
			return err
		}
		i++
	}

	return nil
}
