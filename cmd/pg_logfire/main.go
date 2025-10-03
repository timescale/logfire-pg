package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"

	wire "github.com/jeroenrinzema/psql-wire"
	"github.com/lib/pq/oid"
)

var readToken string
var baseURL = "https://logfire-us.pydantic.dev"

type Column struct {
	Name     string        `json:"name"`
	Datatype interface{}   `json:"datatype"`
	Nullable bool          `json:"nullable"`
	Values   []interface{} `json:"values"`
}

type QueryResponse struct {
	Columns []Column `json:"columns"`
}

func main() {
	flag.Parse()

	if len(flag.Args()) < 1 {
		log.Fatal("Usage: pg_logfire <token>")
	}

	readToken = flag.Args()[0]
	fmt.Println("Starting pg_logfire...")
	fmt.Println("Using read token:", readToken)
	wire.ListenAndServe("127.0.0.1:5432", handler)
}

func handler(ctx context.Context, query string) (wire.PreparedStatements, error) {
	fmt.Println(query)

	req, err := http.NewRequest("GET", baseURL+"/v1/query", nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Authorization", "Bearer "+readToken)

	q := req.URL.Query()
	q.Add("sql", query)
	req.URL.RawQuery = q.Encode()

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("failed to execute query. Status code: %d, body: %s", resp.StatusCode, string(body))
	}

	// Parse the JSON response
	var queryResp QueryResponse
	if err := json.NewDecoder(resp.Body).Decode(&queryResp); err != nil {
		return nil, err
	}

	// Build the columns dynamically
	var columns wire.Columns
	for _, col := range queryResp.Columns {
		columns = append(columns, wire.Column{
			Table: 0,
			Name:  col.Name,
			Oid:   oid.T_text,
			Width: 256,
		})
	}

	// Determine the number of rows (length of values array)
	numRows := 0
	if len(queryResp.Columns) > 0 {
		numRows = len(queryResp.Columns[0].Values)
	}

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
