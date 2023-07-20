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
	DBConfig config.DBConfig
}

// NewPostgresWriter creates a new instance of PostgresWriter.
func NewPostgresWriter(dbConfig config.DBConfig) SQLWriter {
	return &PostgresWriter{
		DBConfig: dbConfig,
	}
}

const (
	POSTGRES  string = "postgres"
	BatchSize int    = 10000
)

func (p *PostgresWriter) WriteSQL(ctx context.Context, sqlChan <-chan string) {
	connStr := fmt.Sprintf(
		"postgresql://%s:%s@%s:%s/%s?sslmode=disable",
		p.DBConfig.UserName,
		p.DBConfig.Password,
		p.DBConfig.Host,
		p.DBConfig.Port,
		p.DBConfig.Name,
	)
	postgresConn, err := sql.Open(POSTGRES, connStr)
	if err != nil {
		panic(err)
	}
	defer postgresConn.Close()

	// Set connection pool properties
	postgresConn.SetMaxOpenConns(10)
	postgresConn.SetMaxIdleConns(5)
	postgresConn.SetConnMaxLifetime(5 * time.Minute)

	err = postgresConn.Ping()
	if err != nil {
		panic(err)
	}

	cnt := 0
	tx, err := postgresConn.Begin()
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

			tx, err = postgresConn.Begin()
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
