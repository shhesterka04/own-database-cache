package parser

import (
	"errors"
	"regexp"
	"strings"
)

func handleInsert(query string) (ParsedQuery, error) {
	re := regexp.MustCompile(`INSERT INTO (\w+)\s*\(([^)]+)\)\s*VALUES\s*(.*)`)
	matches := re.FindStringSubmatch(query)
	if len(matches) != 4 {
		return ParsedQuery{}, errors.New("invalid INSERT INTO query")
	}

	tableName := matches[1]
	columns := strings.Split(matches[2], ",")
	valuesPart := matches[3]

	valuesGroups := strings.Split(valuesPart, "),")
	var values [][]string
	for _, valuesGroup := range valuesGroups {
		value, err := ParseValues(valuesGroup)
		if err != nil {
			return ParsedQuery{}, err
		}
		values = append(values, value)
	}

	return ParsedQuery{
		Operation: "INSERT",
		TableName: tableName,
		Columns:   columns,
		Values:    values,
	}, nil
}
