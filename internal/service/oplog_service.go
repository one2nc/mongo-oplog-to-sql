package service

import (
	"context"
	"encoding/json"
	"sync"

	"github.com/one2nc/mongo-oplog-to-sql/internal/domain"
)

type OplogService interface {
	ProcessOplog(oplog string) []string
	ProcessOplogs(
		oplogChan <-chan domain.OplogEntry,
		cancel context.CancelFunc,
	) chan domain.SQLStatement
	ProcessOplogsConcurrent(
		oplogChan <-chan domain.OplogEntry,
		cancel context.CancelFunc,
	) chan domain.SQLStatement
}

type oplogService struct {
	ctx context.Context

	databaseOplogChanMap map[string]chan domain.OplogEntry

	uuidGenerator domain.UUIDGenerator
}

func NewOplogService(ctx context.Context, uuidGenerator domain.UUIDGenerator) OplogService {
	return &oplogService{
		ctx:                  ctx,
		databaseOplogChanMap: make(map[string]chan domain.OplogEntry),
		uuidGenerator:        uuidGenerator,
	}
}

func (s *oplogService) ProcessOplog(oplogString string) []string {
	var oplogEntries []domain.OplogEntry
	err := json.Unmarshal([]byte(oplogString), &oplogEntries)
	if err != nil {
		var oplogEntry domain.OplogEntry
		err := json.Unmarshal([]byte(oplogString), &oplogEntry)
		if err != nil {
			return []string{}
		}
		oplogEntries = append(oplogEntries, oplogEntry)
	}

	oplopParser := domain.NewOplogParser(s.uuidGenerator)
	cache := domain.NewCache()

	sqlStatements := make([]string, 0)
	for _, entry := range oplogEntries {
		sqls, err := oplopParser.ProcessOplog(entry, cache)
		if err != nil {
			break
		}
		sqlStatements = append(sqlStatements, sqls...)
	}

	return sqlStatements
}

func (s *oplogService) ProcessOplogs(
	oplogChan <-chan domain.OplogEntry,
	cancel context.CancelFunc,
) chan domain.SQLStatement {
	oplopParser := domain.NewOplogParser(s.uuidGenerator)

	sqlChan := make(chan domain.SQLStatement, 1000)
	sqlStmt := domain.NewSQLStatement("1")
	sqlChan <- sqlStmt

	go func() {
		cache := domain.NewCache()
	forLoop:
		for {
			select {
			case oplog, ok := <-oplogChan:
				if !ok {
					// oplogChan is closed, stop reading Oplogs
					break forLoop
				}

				sqls, err := oplopParser.ProcessOplog(oplog, cache)
				if err != nil {
					break
				}

				for _, sql := range sqls {
					sqlStmt.Publish(sql)
				}
			case <-s.ctx.Done():
				// The context is done, stop reading Oplogs
				break forLoop
			}
		}

		// Close the out channel after all values are processed
		sqlStmt.Close()

		cancel()
	}()

	return sqlChan
}

func (s *oplogService) ProcessOplogsConcurrent(
	oplogChan <-chan domain.OplogEntry,
	cancel context.CancelFunc,
) chan domain.SQLStatement {
	oplopParser := domain.NewOplogParser(s.uuidGenerator)

	sqlChan := make(chan domain.SQLStatement, 1000)
	sqlCloseChan := make(chan domain.SQLStatement, 1000)
	var wg sync.WaitGroup

	go func() {
		defer close(sqlChan)
		defer func() {
			close(sqlCloseChan)
			// Close the out channel after all values are processed
			for sqlStmt := range sqlCloseChan {
				sqlStmt.Close()
			}
		}()
		defer cancel()

	Loop:
		for {
			select {
			case oplog, ok := <-oplogChan:
				if !ok {
					break Loop // oplogChan is closed, stop reading Oplogs
				}

				name := oplog.DatabaseName()
				databaseChan, ok := s.databaseOplogChanMap[name]
				if !ok {
					databaseChan = make(chan domain.OplogEntry, 1000)
					sqlStmt := domain.NewSQLStatement(name)

					sqlChan <- sqlStmt
					sqlCloseChan <- sqlStmt

					s.databaseOplogChanMap[name] = databaseChan

					wg.Add(1)
					go oplopParser.ProcessCollectionOplog(databaseChan, sqlStmt, &wg)
				}
				databaseChan <- oplog

			case <-s.ctx.Done():
				break Loop // The context is done, stop reading Oplogs
			}
		}

		for _, collectionOplogChan := range s.databaseOplogChanMap {
			close(collectionOplogChan)
		}

		wg.Wait() // Wait for all collection goroutines to finish
	}()

	return sqlChan
}
