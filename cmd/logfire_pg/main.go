package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"regexp"
	"strings"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
	"github.com/apache/arrow/go/v18/arrow/ipc"
	wire "github.com/jeroenrinzema/psql-wire"
	"github.com/jeroenrinzema/psql-wire/codes"
	psqlerr "github.com/jeroenrinzema/psql-wire/errors"
	"github.com/lib/pq/oid"
	flag "github.com/spf13/pflag"
)

var version = "dev"

var baseURL = "https://logfire-us.pydantic.dev"
var queryUrl = baseURL + "/v1/query"

type PostgreServer struct {
	server *wire.Server
	logger *log.Logger
}

type readTokenCtxKey struct{}

func main() {
	var host string
	var port int
	var showVersion bool
	var showHelp bool

	flag.StringVar(&host, "host", "127.0.0.1", "Host to listen on")
	flag.IntVar(&port, "port", 5432, "Port to listen on")
	flag.BoolVar(&showVersion, "version", false, "Print version and exit")
	flag.BoolVar(&showHelp, "help", false, "Print this help message and exit")
	flag.Parse()

	if showVersion {
		fmt.Printf("logfire_pg %s\n", version)
		os.Exit(0)
	}

	if showHelp {
		flag.Usage()
		os.Exit(0)
	}

	logger := log.New(os.Stdout, "[logfire-pg] ", log.LstdFlags)
	server, err := NewPostgreServer(logger)
	if err != nil {
		logger.Fatalf("failed to create server: %s", err)
	}

	fmt.Println("Starting pg_logfire...")
	err = server.server.ListenAndServe(fmt.Sprintf("%s:%d", host, port))
	if err != nil {
		logger.Fatalf("failed to start server: %s", err)
	}
}

func DetectPsqlCommandQuery(query string) (detectedCommand string, suggestedQuery string, isPsqlCommand bool) {
	// Normalize whitespace for comparison
	normalized := strings.Join(strings.Fields(query), " ")

	// Check for \dt command pattern
	dtPattern := `SELECT n.nspname as "Schema", c.relname as "Name", CASE c.relkind WHEN 'r' THEN 'table' WHEN 'v' THEN 'view' WHEN 'm' THEN 'materialized view' WHEN 'i' THEN 'index' WHEN 'S' THEN 'sequence' WHEN 't' THEN 'TOAST table' WHEN 'f' THEN 'foreign table' WHEN 'p' THEN 'partitioned table' WHEN 'I' THEN 'partitioned index' END as "Type", pg_catalog.pg_get_userbyid(c.relowner) as "Owner" FROM pg_catalog.pg_class c LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace LEFT JOIN pg_catalog.pg_am am ON am.oid = c.relam WHERE c.relkind IN ('r','p','') AND n.nspname <> 'pg_catalog' AND n.nspname !~ '^pg_toast' AND n.nspname <> 'information_schema' AND pg_catalog.pg_table_is_visible(c.oid) ORDER BY 1,2;`

	if normalized == dtPattern {
		return "\\dt", "show tables;", true
	}

	// Check for \d <table> command pattern (without schema)
	dPattern := regexp.MustCompile(`^SELECT c\.oid, n\.nspname, c\.relname FROM pg_catalog\.pg_class c LEFT JOIN pg_catalog\.pg_namespace n ON n\.oid = c\.relnamespace WHERE c\.relname OPERATOR\(pg_catalog\.\~\) '\^\(([^)]+)\)\$' COLLATE pg_catalog\.default AND pg_catalog\.pg_table_is_visible\(c\.oid\) ORDER BY 2, 3;$`)

	if matches := dPattern.FindStringSubmatch(normalized); matches != nil {
		tableName := matches[1]
		return fmt.Sprintf("\\d %s", tableName), fmt.Sprintf("show columns from %s;", tableName), true
	}

	// Check for \d <schema.table> command pattern (with schema)
	dSchemaPattern := regexp.MustCompile(`^SELECT c\.oid, n\.nspname, c\.relname FROM pg_catalog\.pg_class c LEFT JOIN pg_catalog\.pg_namespace n ON n\.oid = c\.relnamespace WHERE c\.relname OPERATOR\(pg_catalog\.\~\) '\^\(([^)]+)\)\$' COLLATE pg_catalog\.default AND n\.nspname OPERATOR\(pg_catalog\.\~\) '\^\(([^)]+)\)\$' COLLATE pg_catalog\.default ORDER BY 2, 3;$`)

	if matches := dSchemaPattern.FindStringSubmatch(normalized); matches != nil {
		tableName := matches[1]
		schemaName := matches[2]
		return fmt.Sprintf("\\d %s.%s", schemaName, tableName), fmt.Sprintf("show columns from %s.%s;", schemaName, tableName), true
	}

	return "", "", false
}

func executeQuery(sql string, token string) (io.ReadCloser, error) {
	req, err := http.NewRequest("GET", queryUrl, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Accept", "application/vnd.apache.arrow.stream")

	q := req.URL.Query()
	q.Add("sql", sql)
	req.URL.RawQuery = q.Encode()

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}

	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		return nil, fmt.Errorf("query failed. Status code: %d, body: %s", resp.StatusCode, string(body))
	}

	// Return the response body as a stream
	return resp.Body, nil
}

func arrowTypeToPgOid(dt arrow.DataType) (oid.Oid, error) {
	switch dt.ID() {
	case arrow.STRING, arrow.LARGE_STRING:
		return oid.T_text, nil
	case arrow.BOOL:
		return oid.T_bool, nil
	case arrow.INT32:
		return oid.T_int4, nil
	case arrow.INT64:
		return oid.T_int8, nil
	case arrow.UINT16:
		return oid.T_int4, nil
	case arrow.UINT32:
		return oid.T_int8, nil
	case arrow.UINT64:
		return oid.T_int8, nil
	case arrow.FLOAT64:
		return oid.T_float8, nil
	case arrow.DATE32:
		return oid.T_date, nil
	case arrow.TIMESTAMP:
		return oid.T_timestamptz, nil
	case arrow.LIST:
		listType := dt.(*arrow.ListType)
		innerOid, err := arrowTypeToPgOid(listType.Elem())
		if err != nil {
			return 0, err
		}
		// Convert to array OID (add underscore prefix)
		switch innerOid {
		case oid.T_text:
			return oid.T__text, nil
		case oid.T_bool:
			return oid.T__bool, nil
		case oid.T_int4:
			return oid.T__int4, nil
		case oid.T_int8:
			return oid.T__int8, nil
		case oid.T_float8:
			return oid.T__float8, nil
		case oid.T_date:
			return oid.T__date, nil
		default:
			return 0, fmt.Errorf("unsupported list inner type: %v", innerOid)
		}
	default:
		return 0, fmt.Errorf("unsupported arrow type: %v", dt)
	}
}

func arrowValueToInterface(col arrow.Array, rowIdx int) (interface{}, error) {
	if col.IsNull(rowIdx) {
		return nil, nil
	}

	switch arr := col.(type) {
	case *array.String:
		return arr.Value(rowIdx), nil
	case *array.Boolean:
		return arr.Value(rowIdx), nil
	case *array.Int32:
		return float64(arr.Value(rowIdx)), nil
	case *array.Int64:
		return float64(arr.Value(rowIdx)), nil
	case *array.Uint16:
		return float64(arr.Value(rowIdx)), nil
	case *array.Uint32:
		return float64(arr.Value(rowIdx)), nil
	case *array.Uint64:
		return float64(arr.Value(rowIdx)), nil
	case *array.Float64:
		return arr.Value(rowIdx), nil
	case *array.Date32:
		return arr.Value(rowIdx).FormattedString(), nil
	case *array.Timestamp:
		return arr.Value(rowIdx).ToTime(arrow.Microsecond).Format("2006-01-02T15:04:05.000000Z"), nil
	case *array.List:
		listValues := make([]interface{}, 0)
		start, end := arr.ValueOffsets(rowIdx)
		innerArray := arr.ListValues()

		for j := range int(end) - int(start) {
			val, err := arrowValueToInterface(innerArray, int(start)+j)
			if err != nil {
				return nil, err
			}
			listValues = append(listValues, val)
		}

		// Convert to JSON string for PostgreSQL array representation
		jsonBytes, _ := json.Marshal(listValues)
		return string(jsonBytes), nil
	default:
		return nil, fmt.Errorf("unsupported arrow type: %T", arr)
	}
}

func NewPostgreServer(logger *log.Logger) (*PostgreServer, error) {
	server := &PostgreServer{
		logger: logger,
	}

	wireServer, err := wire.NewServer(
		server.wireHandler,
		wire.SessionAuthStrategy(wire.ClearTextPassword(server.auth)),
		wire.SessionMiddleware(server.session),
		wire.TerminateConn(server.terminateConn),
		wire.Version("17.0"),
	)
	if err != nil {
		return nil, err
	}
	server.server = wireServer
	return server, nil
}

func (s *PostgreServer) auth(ctx context.Context, database, username, password string) (context.Context, bool, error) {
	if username == "" {
		return ctx, false, fmt.Errorf("username cannot be empty")
	}

	// Validate password by making API call to logfire
	respBody, err := executeQuery("SELECT 1", password)
	if err != nil {
		return ctx, false, fmt.Errorf("authentication failed: %w", err)
	}
	// Close immediately as we just need to verify the token works
	respBody.Close()

	ctx = context.WithValue(ctx, readTokenCtxKey{}, password)

	s.logger.Printf("successful authentication for user: %s", username)
	return ctx, true, nil
}

// session middleware for handling session context
func (s *PostgreServer) session(ctx context.Context) (context.Context, error) {
	s.logger.Printf("new session established: %s", wire.RemoteAddress(ctx))
	return ctx, nil
}

// terminateConn handles connection termination
func (s *PostgreServer) terminateConn(ctx context.Context) error {
	s.logger.Printf("session terminated: %s", wire.RemoteAddress(ctx))
	return nil
}

// wireHandler processes incoming SQL queries
func (s *PostgreServer) wireHandler(ctx context.Context, query string) (wire.PreparedStatements, error) {
	s.logger.Printf("incoming SQL query: %s", query)

	detectedCommand, suggestedQuery, isPsqlCommand := DetectPsqlCommandQuery(query)
	if isPsqlCommand {
		s.logger.Printf("detected psql command %s, suggesting alternative: %s", detectedCommand, suggestedQuery)
		return nil, psqlerr.WithSeverity(
			psqlerr.WithCode(
				fmt.Errorf("psql commands are not supported. Detected trying to use: %s. Please run instead:\n\n%s", detectedCommand, suggestedQuery),
				codes.FeatureNotSupported,
			),
			psqlerr.LevelError,
		)
	}

	readToken := ctx.Value(readTokenCtxKey{}).(string)
	respBody, err := executeQuery(query, readToken)
	if err != nil {
		s.logger.Printf("query execution error: %v", err)
		return nil, psqlerr.WithSeverity(psqlerr.WithCode(err, codes.SyntaxErrorOrAccessRuleViolation), psqlerr.LevelFatal)
	}

	// Create Arrow IPC reader from the response stream
	reader, err := ipc.NewReader(respBody)
	if err != nil {
		respBody.Close()
		s.logger.Printf("failed to create arrow reader: %v", err)
		return nil, psqlerr.WithSeverity(psqlerr.WithCode(err, codes.DataException), psqlerr.LevelFatal)
	}

	// Extract column information from schema
	schema := reader.Schema()
	var columns wire.Columns
	for _, field := range schema.Fields() {
		pgOid, err := arrowTypeToPgOid(field.Type)
		if err != nil {
			reader.Release()
			respBody.Close()
			s.logger.Printf("type mapping error for column %s: %v", field.Name, err)
			return nil, psqlerr.WithSeverity(psqlerr.WithCode(err, codes.DatatypeMismatch), psqlerr.LevelFatal)
		}

		columns = append(columns, wire.Column{
			Table: 0,
			Name:  field.Name,
			Oid:   pgOid,
			Width: 256,
		})
	}

	// Build the handler that streams rows from Arrow batches
	handle := func(ctx context.Context, writer wire.DataWriter, parameters []wire.Parameter) error {
		defer reader.Release()
		defer respBody.Close()

		totalRows := 0

		// Stream through all record batches
		for reader.Next() {
			record := reader.Record()
			numRows := int(record.NumRows())
			numCols := int(record.NumCols())

			// Process each row in the batch
			for i := range numRows {
				row := make([]any, numCols)

				// Extract values for each column
				for j := range numCols {
					col := record.Column(j)
					val, err := arrowValueToInterface(col, i)
					if err != nil {
						return fmt.Errorf("failed to convert column %d row %d: %w", j, i, err)
					}
					row[j] = val
				}

				writer.Row(row)
				totalRows++
			}
		}

		if err := reader.Err(); err != nil {
			return fmt.Errorf("error reading arrow stream: %w", err)
		}

		return writer.Complete(fmt.Sprintf("SELECT %d", totalRows))
	}

	return wire.Prepared(wire.NewStatement(handle, wire.WithColumns(columns))), nil
}
