package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/one2nc/mongo-oplog-to-sql/internal/domain"
	"github.com/one2nc/mongo-oplog-to-sql/internal/reader"
	"github.com/one2nc/mongo-oplog-to-sql/internal/service"
	"github.com/one2nc/mongo-oplog-to-sql/internal/writer"
	"github.com/spf13/cobra"
)

func main() {
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}

var oplogFile string
var sqlFile string

func init() {
	rootCmd.Flags().StringVarP(&oplogFile, "source_file", "f", "", "Source oplog file")
	rootCmd.Flags().StringVarP(&sqlFile, "target_file", "o", "", "Target SQL file")
}

var rootCmd = &cobra.Command{
	Use:   "MongoOplogToSQL",
	Short: "A utility for parsing MongoDB's oplog and translating it into equivalent SQL statements",
	Long:  `MongoOplogToSQL is a powerful utility that allows you to parse the oplog data from MongoDB and effortlessly translate it into SQL statements. With this tool, you can seamlessly migrate your data from MongoDB to a SQL-based database system while preserving the integrity and structure of your data. Say goodbye to manual migration efforts and let MongoOplogToSQL automate the process for you.`,
	Run: func(cmd *cobra.Command, args []string) {
		if !cmd.Flags().HasFlags() {
			cmd.Usage()
			return
		}

		// Create a context that will be cancelled on interrupt signal
		ctx, cancel := context.WithCancel(context.Background())

		// Handle interrupt signal
		handleInterruptSignal(cancel)

		publisher := domain.NewInMemoryOplogPublisher()

		// Get oplogs from publisher
		oplogChan, err := publisher.GetOplogs()
		if err != nil {
			cancel()
			return
		}

		// Create a reader to read the oplogs
		oplogReader := reader.NewFileReader(oplogFile)

		// Start reading Oplog entries in a separate goroutine
		go oplogReader.ReadOplogs(ctx, publisher)

		// Create a service to process the oplogs
		oplogService := service.NewOplogService(ctx, domain.NewDefaultUUIDGenerator())
		sqlChan := oplogService.ProcessOplogs(oplogChan, cancel)

		// Create a writer to write the sql statements
		sqlWriter := writer.NewFileWriter(sqlFile)
		sqlWriter.WriteSQL(ctx, sqlChan)
	},
}

func handleInterruptSignal(cancel context.CancelFunc) {
	// Create an interrupt channel to listen for the interrupt signal
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-interrupt
		fmt.Println("Interrupt signal received. Gracefully stopping...")

		// Cancel the context to signal the shutdown
		cancel()
	}()
}