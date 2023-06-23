package writer

import "context"

type SQLWriter interface {
	WriteSQL(ctx context.Context, sqlChan <-chan string)
}
