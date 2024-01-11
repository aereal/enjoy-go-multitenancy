package adapters

import (
	"context"
	"database/sql/driver"
	"fmt"
	"log/slog"
	"net"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/XSAM/otelsql"
	"github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/jmoiron/sqlx"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	_ "github.com/pingcap/tidb/pkg/parser/test_driver"
	"go.opentelemetry.io/otel/attribute"
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

func OpenEventsDB(dsn string) (*sqlx.DB, error) {
	parsed, err := url.Parse(dsn)
	if err != nil {
		return nil, fmt.Errorf("url.Parse: %w", err)
	}
	attrs := make([]attribute.KeyValue, 0)
	attrs = append(attrs, semconv.DBSystemPostgreSQL)
	if userinfo := parsed.User; userinfo != nil {
		attrs = append(attrs, semconv.DBUser(userinfo.Username()))
	}
	if hostname := parsed.Hostname(); hostname != "" {
		attrs = append(attrs, semconv.NetTransportTCP, semconv.ServerAddress(hostname))
	}
	spanOptions := otelsql.SpanOptions{DisableErrSkip: true, SpanFilter: filterSpanForPostgres}
	return open("pgx", dsn, spanOptions, attrs...)
}

func OpenDB(dsn string) (*sqlx.DB, error) {
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		return nil, fmt.Errorf("mysql.ParseDSN: %w", err)
	}
	cfg.ParseTime = true
	cfg.Loc = dbLoc
	return open("mysql", cfg.FormatDSN(), otelsql.SpanOptions{DisableErrSkip: true, SpanFilter: filterSpanForMySQL}, buildDefaultAttrs(cfg)...)
}

func open(driverName string, dsn string, spanOptions otelsql.SpanOptions, attrs ...attribute.KeyValue) (*sqlx.DB, error) {
	store := &queryStore{queryCache: &mapCache{dirty: make(map[string]ast.StmtNode)}}
	db, err := otelsql.Open(driverName, dsn,
		otelsql.WithAttributes(attrs...),
		otelsql.WithAttributesGetter(store.attributesGetter),
		otelsql.WithSpanNameFormatter(store.spanNameFormatter),
		otelsql.WithSpanOptions(spanOptions))
	if err != nil {
		return nil, fmt.Errorf("otelsql.Open: %w", err)
	}
	return sqlx.NewDb(db, driverName), nil
}

func filterSpanForMySQL(_ context.Context, method otelsql.Method, _ string, _ []driver.NamedValue) bool {
	switch method {
	case otelsql.MethodRows, otelsql.MethodConnPrepare, otelsql.MethodConnPing, otelsql.MethodConnResetSession, otelsql.MethodConnQuery, otelsql.MethodConnExec:
		return false
	}
	return true
}

func filterSpanForPostgres(_ context.Context, method otelsql.Method, _ string, _ []driver.NamedValue) bool {
	switch method {
	case otelsql.MethodRows, otelsql.MethodConnPrepare, otelsql.MethodConnPing, otelsql.MethodConnResetSession:
		return false
	}
	return true
}

func buildDefaultAttrs(cfg *mysql.Config) []attribute.KeyValue {
	attrs := make([]attribute.KeyValue, 0, 6)
	attrs = append(attrs,
		semconv.DBName(cfg.DBName),
		semconv.DBUser(cfg.User),
		semconv.DBSystemMySQL,
	)
	var isNet bool
	switch cfg.Net {
	case "tcp":
		isNet = true
		attrs = append(attrs, semconv.NetTransportTCP)
	case "udp":
		isNet = true
		attrs = append(attrs, semconv.NetTransportUDP)
	}
	if !isNet {
		return attrs
	}
	host, portStr, ok := extractHost(cfg.Addr)
	if !ok {
		return attrs
	}
	if ips, err := net.LookupIP(host); err == nil {
		attrs = append(attrs, semconv.ServerAddress(ips[0].String()))
	}
	if port, err := strconv.Atoi(portStr); err == nil {
		attrs = append(attrs, semconv.ServerPort(port))
	}
	return attrs
}

func extractHost(addr string) (string, string, bool) {
	host, port, err := net.SplitHostPort(addr)
	if err == nil {
		return host, port, true
	}
	if !strings.HasSuffix(err.Error(), "") {
		return "", "", false
	}
	host, port, err = net.SplitHostPort(addr + ":3306")
	if err != nil {
		return "", "", false
	}
	return host, port, true
}

type visitor struct {
	sync.Mutex
	tables    []string
	operation string
}

var _ ast.Visitor = (*visitor)(nil)

const (
	opDelete = "DELETE"
	opInsert = "INSERT"
	opUpdate = "UPDATE"
	opSelect = "SELECT"
)

func (v *visitor) Enter(n ast.Node) (ast.Node, bool) {
	switch n := n.(type) {
	case *ast.TableName:
		v.Lock()
		defer v.Unlock()
		v.tables = append(v.tables, n.Name.O)
	case *ast.DeleteStmt:
		v.Lock()
		defer v.Unlock()
		v.operation = opDelete
	case *ast.InsertStmt:
		v.Lock()
		defer v.Unlock()
		v.operation = opInsert
	case *ast.UpdateStmt:
		v.Lock()
		defer v.Unlock()
		v.operation = opUpdate
	case *ast.SelectStmt:
		v.Lock()
		defer v.Unlock()
		v.operation = opSelect
	}
	return n, false
}

func (v *visitor) Leave(n ast.Node) (ast.Node, bool) {
	return n, true
}

type queryStore struct {
	queryCache
}

func (s *queryStore) spanNameFormatter(ctx context.Context, method otelsql.Method, query string) string {
	stmt := s.parseWithCache(ctx, query)
	if stmt == nil {
		return string(method)
	}
	visitor := &visitor{}
	_, _ = stmt.Accept(visitor)
	if visitor.operation == "" {
		return string(method)
	}
	if len(visitor.tables) == 0 {
		return fmt.Sprintf("%s %s", method, visitor.operation)
	}
	switch method {
	case otelsql.MethodConnQuery, otelsql.MethodStmtQuery, otelsql.MethodConnExec, otelsql.MethodStmtExec:
		return fmt.Sprintf("%s %s", visitor.operation, visitor.tables[0])
	}
	return fmt.Sprintf("%s %s %s", method, visitor.operation, visitor.tables[0])
}

func (s *queryStore) attributesGetter(ctx context.Context, method otelsql.Method, query string, _ []driver.NamedValue) []attribute.KeyValue {
	stmt := s.parseWithCache(ctx, query)
	if stmt == nil {
		return nil
	}
	visitor := &visitor{}
	_, _ = stmt.Accept(visitor)
	var n int
	n += len(visitor.tables)
	if visitor.operation != "" {
		n++
	}
	attrs := make([]attribute.KeyValue, 0, n)
	for _, t := range visitor.tables {
		attrs = append(attrs, semconv.DBSQLTable(t))
	}
	if visitor.operation != "" {
		attrs = append(attrs, semconv.DBOperation(visitor.operation))
	}
	return attrs
}

func (s *queryStore) parseWithCache(ctx context.Context, query string) ast.StmtNode {
	if query == "" {
		return nil
	}
	stmt, ok := s.queryCache.Get(ctx, query)
	if !ok {
		parser := parser.New()
		stmts, _, err := parser.ParseSQL(query)
		if err != nil {
			slog.DebugContext(ctx, "failed to parse query", slog.String("query", query), slog.String("error", err.Error()))
			return nil
		}
		slog.DebugContext(ctx, "parsed query", slog.Int("statement_num", len(stmts)))
		if len(stmts) == 0 {
			return nil
		}
		if len(stmts) > 1 {
			slog.WarnContext(ctx, "multiple statement is not supported; rest of statements are ignored")
		}
		stmt = stmts[0]
		s.queryCache.Set(ctx, query, stmt)
	}
	return stmt
}

type queryCache interface {
	Get(ctx context.Context, query string) (ast.StmtNode, bool)
	Set(ctx context.Context, query string, node ast.StmtNode)
}

type mapCache struct {
	sync.RWMutex
	dirty map[string]ast.StmtNode
}

var _ queryCache = (*mapCache)(nil)

func (qc *mapCache) Get(ctx context.Context, query string) (ast.StmtNode, bool) {
	qc.RLock()
	defer qc.RUnlock()
	v, ok := qc.dirty[query]
	slog.DebugContext(ctx, "cache get", slog.Bool("hit", ok && v != nil))
	return v, ok
}

func (qc *mapCache) Set(_ context.Context, query string, node ast.StmtNode) {
	qc.Lock()
	defer qc.Unlock()
	qc.dirty[query] = node
}
