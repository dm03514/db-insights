package metrics

import "context"

type TableColumn struct {
	Database string
	Schema   string
	Table    string
	Column   string
	Type     string
	Length   int
}

type Columns interface {
	Columns(ctx context.Context) ([]TableColumn, error)
}

type ColumnChecker struct {
}
