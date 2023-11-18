package parser

import (
	"errors"
	"regexp"
	"strings"
)

func handleCreate(query string) (ParsedQuery, error) {
	re := regexp.MustCompile(`CREATE TABLE (\w+)\s*\(([^)]+)\)\s*WITH TYPES\s*\(([^)]+)\)`)
	matches := re.FindStringSubmatch(query)
	if len(matches) != 4 {
		return ParsedQuery{}, errors.New("invalid CREATE TABLE query")
	}

	tableName := matches[1]
	columns := strings.Split(matches[2], ",")
	for i, col := range columns {
		columns[i] = strings.TrimSpace(col)
	}
	dataTypes := strings.Split(matches[3], ",")
	for i, dt := range dataTypes {
		dataTypes[i] = strings.TrimSpace(dt)
	}

	return ParsedQuery{
		Operation: "CREATE",
		TableName: tableName,
		Columns:   columns,
		Values:    [][]string{dataTypes},
	}, nil
}