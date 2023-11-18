package parser

import (
	"errors"
	"strings"
)
type ParsedQuery struct {
    Operation    string
    TableName    string
    Columns      []string
    Values       [][]string
    WhereClause  string
    OrderByClause string
}

func ParseSQL(query string) (ParsedQuery, error) {
	var parsedQuery ParsedQuery

	tokens := strings.Fields(query)
	if len(tokens) == 0 {
		return parsedQuery, errors.New("empty query")
	}

	switch strings.ToUpper(tokens[0]) {
	case "SELECT":
		return handleSelect(query)
	case "INSERT":
		return handleInsert(query)
	case "UPDATE":
		return handleUpdate(query)
	case "DELETE":
		return handleDelete(query)
	case "CREATE":
		return handleCreate(query)
	default:
		return parsedQuery, errors.New("unsupported query")
	}
}
