package adapters

import (
	"fmt"
	"time"

	"github.com/XSAM/otelsql"
	"github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	semconv "go.opentelemetry.io/otel/semconv/v1.21.0"
)

var dbLoc *time.Location

func init() {
	var err error
	dbLoc, err = time.LoadLocation("Asia/Tokyo")
	if err != nil {
		panic(err)
	}
}

const driverName = "mysql"

func OpenDB(dsn string) (*sqlx.DB, error) {
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		return nil, fmt.Errorf("mysql.ParseDSN: %w", err)
	}
	cfg.ParseTime = true
	cfg.Loc = dbLoc
	db, err := otelsql.Open(driverName, cfg.FormatDSN(),
		otelsql.WithAttributes(semconv.DBName(cfg.DBName)),
		otelsql.WithSpanOptions(otelsql.SpanOptions{Ping: true, DisableErrSkip: true}))
	if err != nil {
		return nil, fmt.Errorf("otelsql.Open: %w", err)
	}
	return sqlx.NewDb(db, driverName), nil
}
