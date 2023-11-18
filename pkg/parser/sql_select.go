package parser

import (
	"errors"
	"regexp"
	"strings"
)

func handleSelect(query string) (ParsedQuery, error) {
	re := regexp.MustCompile(`SELECT\s+(.*?)\s+FROM\s+(\w+)(?:\s+WHERE\s+(.*?))?(?:\s+ORDER\s+BY\s+(.*?))?$`)
	matches := re.FindStringSubmatch(query)
	if matches == nil {
		return ParsedQuery{}, errors.New("invalid SELECT query")
	}

	columns := strings.Split(matches[1], ",")
	for i := range columns {
		columns[i] = strings.TrimSpace(columns[i])
	}
	tableName := matches[2]
	whereClause := matches[3]
	orderByClause := matches[4]

	return ParsedQuery{
		Operation:     "SELECT",
		TableName:     tableName,
		Columns:       columns,
		WhereClause:   whereClause,
		OrderByClause: orderByClause,
	}, nil
}
