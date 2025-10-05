package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"

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

type Column struct {
	Name     string        `json:"name"`
	Datatype interface{}   `json:"datatype"`
	Nullable bool          `json:"nullable"`
	Values   []interface{} `json:"values"`
}

type QueryResponse struct {
	Columns []Column `json:"columns"`
}

type readTokenCtxKey struct{}

func main() {
	var port int
	var showVersion bool
	var showHelp bool

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

	logger := log.New(os.Stdout, "[psql-wire] ", log.LstdFlags)
	server, err := NewPostgreServer(logger)
	if err != nil {
		logger.Fatalf("failed to create server: %s", err)
	}

	fmt.Println("Starting pg_logfire...")
	err = server.server.ListenAndServe(fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		logger.Fatalf("failed to start server: %s", err)
	}
}

func executeQuery(sql string, token string) (*QueryResponse, error) {
	req, err := http.NewRequest("GET", queryUrl, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Authorization", "Bearer "+token)

	q := req.URL.Query()
	q.Add("sql", sql)
	req.URL.RawQuery = q.Encode()

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("query failed. Status code: %d, body: %s", resp.StatusCode, string(body))
	}

	// Parse the JSON response
	var queryResp QueryResponse
	if err := json.NewDecoder(resp.Body).Decode(&queryResp); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}

	return &queryResp, nil
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
	queryResp, err := executeQuery("SELECT 1", password)
	if err != nil {
		return ctx, false, fmt.Errorf("authentication failed: %w", err)
	}

	// Validate response structure
	if len(queryResp.Columns) != 1 {
		return ctx, false, fmt.Errorf("expected 1 column, got %d", len(queryResp.Columns))
	}

	if len(queryResp.Columns[0].Values) != 1 {
		return ctx, false, fmt.Errorf("expected 1 value, got %d", len(queryResp.Columns[0].Values))
	}

	// Check if the value is 1
	v, ok := queryResp.Columns[0].Values[0].(float64)
	if !ok {
		return ctx, false, fmt.Errorf("expected integer value, got %v (type %T)", queryResp.Columns[0].Values[0], queryResp.Columns[0].Values[0])
	}
	if v != 1.0 {
		return ctx, false, fmt.Errorf("expected value 1, got %v", v)
	}

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

	readToken := ctx.Value(readTokenCtxKey{}).(string)
	queryResp, err := executeQuery(query, readToken)
	if err != nil {
		s.logger.Printf("query execution error: %v", err)
		return nil, psqlerr.WithSeverity(psqlerr.WithCode(err, codes.SyntaxErrorOrAccessRuleViolation), psqlerr.LevelFatal)
	}

	var columns wire.Columns
	for _, col := range queryResp.Columns {
		pgOid, err := arrowTypeToPgOid(col.Name, col.Datatype)
		if err != nil {
			s.logger.Printf("type mapping error: %v", err)
			return nil, psqlerr.WithSeverity(psqlerr.WithCode(err, codes.DatatypeMismatch), psqlerr.LevelFatal)
		}

		columns = append(columns, wire.Column{
			Table: 0,
			Name:  col.Name,
			Oid:   pgOid,
			Width: 256,
		})
	}

	numRows := 0
	if len(queryResp.Columns) > 0 {
		numRows = len(queryResp.Columns[0].Values)
	}

	// Build the columns dynamically
	handle := func(ctx context.Context, writer wire.DataWriter, parameters []wire.Parameter) error {
		// Write each row
		for i := 0; i < numRows; i++ {
			row := make([]any, len(queryResp.Columns))
			for j, col := range queryResp.Columns {
				val := col.Values[i]
				// Convert to string if not already
				if str, ok := val.(string); ok {
					row[j] = str
				} else if val == nil {
					row[j] = nil
				} else {
					// For numbers, bools, or JSON objects, marshal to string
					jsonBytes, _ := json.Marshal(val)
					row[j] = string(jsonBytes)
				}
			}
			writer.Row(row)
		}
		return writer.Complete(fmt.Sprintf("SELECT %d", numRows))
	}

	return wire.Prepared(wire.NewStatement(handle, wire.WithColumns(columns))), nil
}

// arrowTypeToPgOid maps Arrow data types to PostgreSQL OID types
func arrowTypeToPgOid(columnName string, datatype interface{}) (oid.Oid, error) {
	// Handle simple string types
	if typeStr, ok := datatype.(string); ok {
		switch typeStr {
		case "Utf8":
			return oid.T_text, nil
		case "JSON":
			return oid.T_json, nil
		case "Boolean":
			return oid.T_bool, nil
		case "Int32":
			return oid.T_int4, nil
		case "Int64":
			return oid.T_int8, nil
		case "UInt16":
			return oid.T_int4, nil
		case "UInt32":
			return oid.T_int8, nil
		case "Float64":
			return oid.T_float8, nil
		case "Date32":
			return oid.T_date, nil
		default:
			return 0, fmt.Errorf("column '%s' has unsupported datatype: %s", columnName, typeStr)
		}
	}

	// Handle complex types (maps and arrays)
	if typeMap, ok := datatype.(map[string]interface{}); ok {
		// Check for List type: {"List": {"name": "item", "datatype": "Utf8", "nullable": true}}
		if listDef, hasListKey := typeMap["List"]; hasListKey {
			// Extract the inner datatype from the List definition
			if listMap, ok := listDef.(map[string]interface{}); ok {
				if innerType, ok := listMap["datatype"].(string); ok {
					switch innerType {
					case "Utf8":
						return oid.T__text, nil
					case "JSON":
						return oid.T__json, nil
					case "Boolean":
						return oid.T__bool, nil
					case "Int32":
						return oid.T__int4, nil
					case "Int64":
						return oid.T__int8, nil
					case "UInt16":
						return oid.T__int4, nil
					case "UInt32":
						return oid.T__int8, nil
					case "Float64":
						return oid.T__float8, nil
					case "Date32":
						return oid.T__date, nil
					default:
						return 0, fmt.Errorf("column '%s' has unsupported List inner datatype: %s", columnName, innerType)
					}
				}
			}
			return 0, fmt.Errorf("column '%s' has invalid List definition: %v", columnName, listDef)
		}

		// Check for Timestamp type: {"Timestamp": ["Microsecond", "UTC"]}
		if _, hasTimestampKey := typeMap["Timestamp"]; hasTimestampKey {
			// Map Arrow Timestamp to PostgreSQL timestamptz
			return oid.T_timestamptz, nil
		}

		return 0, fmt.Errorf("column '%s' has unsupported complex datatype: %v", columnName, typeMap)
	}

	// Handle array types (for Timestamp: ["Microsecond", "UTC"])
	if typeArr, ok := datatype.([]interface{}); ok {
		if len(typeArr) >= 2 {
			if first, ok := typeArr[0].(string); ok && first == "Microsecond" {
				// This is a Timestamp type
				return oid.T_timestamptz, nil
			}
		}
		return 0, fmt.Errorf("column '%s' has unsupported array datatype: %v", columnName, typeArr)
	}

	return 0, fmt.Errorf("column '%s' has invalid datatype format: expected string, map, or array, got %T", columnName, datatype)
}
