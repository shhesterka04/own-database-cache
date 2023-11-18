package parser

import (
	"errors"
	"fmt"
	"strings"
)

func ParseValues(input string) ([]string, error) {
	var values []string
	var value string
	inQuotes := false

	for _, r := range input {
		switch r {
		case '\'':
			if inQuotes {
				trimmedValue := strings.TrimSpace(value)
				values = append(values, trimmedValue)
				value = ""
			}
			inQuotes = !inQuotes
		case ',':
			if !inQuotes {
				trimmedValue := strings.TrimSpace(value)
				if trimmedValue != "" {
					values = append(values, strings.Trim(trimmedValue, "()"))
				}
				value = ""
			} else {
				value += string(r)
			}
		case '(':
			if !inQuotes {
				continue
			}
			value += string(r)
		default:
			if inQuotes || r != ')' {
				value += string(r)
			}
		}
	}

	if inQuotes {
		return nil, errors.New("mismatched quotes in values")
	}

	if value != "" {
		trimmedValue := strings.TrimSpace(value)
		values = append(values, strings.Trim(trimmedValue, "()"))
	}

	return values, nil
}

func ParseWhereClause(whereClause string, header []string) (int64, string, error) {
	parts := strings.Split(whereClause, "=")
	if len(parts) != 2 {
		return 0, "", fmt.Errorf("invalid WHERE clause")
	}

	column := strings.TrimSpace(parts[0])
	value := strings.TrimSpace(parts[1])
	var columnIndex int64
	columnIndex = -1
	for i, h := range header {
		if h == column {
			columnIndex = int64(i)
			break
		}
	}
	if columnIndex == -1 {
		return 0, "", fmt.Errorf("column %s not found", column)
	}

	return columnIndex, value, nil
}
