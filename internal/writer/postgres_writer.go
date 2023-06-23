package writer

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "github.com/lib/pq"
	"github.com/one2nc/mongo-oplog-to-sql/config"
)

// PostgresWriter implements the SQLWriter interface for writing SQL commands to the postgres DB.
type PostgresWriter struct {
	dbConn *sql.DB
}

// NewPostgresWriter creates a new instance of PostgresWriter.
func NewPostgresWriter(dbConfig config.DBConfig) SQLWriter {
	connStr := fmt.Sprintf(
		"postgresql://%s:%s@%s:%s/%s?sslmode=disable",
		dbConfig.UserName,
		dbConfig.Password,
		dbConfig.Host,
		dbConfig.Port,
		dbConfig.Name,
	)
	postgresConn, err := sql.Open(POSTGRES, connStr)
	if err != nil {
		panic(err)
	}

	// Set connection pool properties
	postgresConn.SetMaxOpenConns(10)
	postgresConn.SetMaxIdleConns(5)
	postgresConn.SetConnMaxLifetime(5 * time.Minute)

	err = postgresConn.Ping()
	if err != nil {
		panic(err)
	}

	return &PostgresWriter{
		dbConn: postgresConn,
	}
}

const (
	POSTGRES  string = "postgres"
	BatchSize int    = 10000
)

func (p *PostgresWriter) WriteSQL(ctx context.Context, sqlChan <-chan string) {
	defer p.dbConn.Close()

	cnt := 0
	tx, err := p.dbConn.Begin()
	if err != nil {
		panic(err)
	}

	for sqlCmd := range sqlChan {
		// Check if the context is done
		select {
		case <-ctx.Done():
			// The context is done, stop reading Oplogs
			return
		default:
			// Context is still active, continue reading Oplogs
		}

		cnt++
		if cnt%BatchSize == 0 {

			err = tx.Commit()
			if err != nil {
				panic(err)
			}

			tx, err = p.dbConn.Begin()
			if err != nil {
				panic(err)
			}
		}

		_, err = tx.Exec(sqlCmd)
		println(sqlCmd)
		if err != nil {
			tx.Rollback()
			panic(err)
		}

	}

	// Commit the remaining queries
	if err := tx.Commit(); err != nil {
		panic(err)
	}
}
