package writer

import (
	"bufio"
	"context"
	"fmt"
	"os"
)

// FileWriter implements the SQLWriter interface for writing SQL commands to the file.
type FileWriter struct {
	FilePath string
}

// NewFileWriter creates a new instance of FileWriter.
func NewFileWriter(filePath string) SQLWriter {
	return &FileWriter{
		FilePath: filePath,
	}
}

func (f *FileWriter) WriteSQL(ctx context.Context, sqlChan <-chan string) {
	outputFile, err := os.OpenFile(f.FilePath, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer outputFile.Close()

	writer := bufio.NewWriter(outputFile)
	defer func() {
		err = writer.Flush()
		if err != nil {
			fmt.Println(err)
			return
		}
	}()

	for sqlCmd := range sqlChan {
		// Check if the context is done
		select {
		case <-ctx.Done():
			// The context is done, stop reading Oplogs
			return
		default:
			// Context is still active, continue reading Oplogs
		}

		_, err := writer.WriteString(fmt.Sprintf("%s\n", sqlCmd))
		println(sqlCmd)
		if err != nil {
			fmt.Println(err)
			return
		}
	}
}
