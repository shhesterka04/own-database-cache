package parser

import (
	"errors"
	"regexp"
)

func handleDelete(query string) (ParsedQuery, error) {
	re := regexp.MustCompile(`DELETE\s+FROM\s+(\w+)(?:\s+WHERE\s+(.*))?`)
	matches := re.FindStringSubmatch(query)
	if matches == nil {
		return ParsedQuery{}, errors.New("invalid DELETE query")
	}

	tableName := matches[1]
	whereClause := ""
	if len(matches) > 2 {
		whereClause = matches[2]
	}

	return ParsedQuery{
		Operation:   "DELETE",
		TableName:   tableName,
		WhereClause: whereClause,
	}, nil
}
