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

	writeFlushCntr := 0
	for sqlCmd := range sqlChan {
		_, err := writer.WriteString(fmt.Sprintf("%s\n", sqlCmd))
		println(sqlCmd)
		if err != nil {
			fmt.Println(err)
			return
		}

		// this will act as a counter to flush
		writeFlushCntr++
		// deciding threshold of writer to flush
		if writeFlushCntr%10 == 0 {
			err := writer.Flush()
			if err != nil {
				fmt.Println(err)
				return
			}
		}
	}

	err = writer.Flush()
	if err != nil {
		fmt.Println(err)
		return
	}
}