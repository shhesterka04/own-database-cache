package parser

import (
	"errors"
	"regexp"
	"strings"
)

func handleUpdate(query string) (ParsedQuery, error) {
	re := regexp.MustCompile(`UPDATE\s+(\w+)\s+SET\s+(.*?)(?:\s+WHERE\s+(.*))?$`)
	matches := re.FindStringSubmatch(query)
	if matches == nil {
		return ParsedQuery{}, errors.New("неверный запрос UPDATE")
	}

	tableName := matches[1]
	setClause := matches[2]
	whereClause := ""
	if len(matches) > 3 {
		whereClause = matches[3]
	}

	setParts := strings.Split(setClause, ",")
	var sets []string
	for _, part := range setParts {
		set := strings.TrimSpace(part)
		sets = append(sets, set)
	}

	return ParsedQuery{
		Operation:   "UPDATE",
		TableName:   tableName,
		Values:      [][]string{sets},
		WhereClause: whereClause,
	}, nil
}
