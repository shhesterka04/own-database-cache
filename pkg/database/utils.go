package database

import (
	"bufio"
	"encoding/csv"
	"errors"
	"os"
	"sort"
	"strconv"
	"strings"
)

func checkDataType(value, dataType string) error {
	switch dataType {
	case "int64":
		if _, err := strconv.ParseInt(value, 10, 64); err != nil {
			return errors.New("invalid type: expected int64")
		}
	case "string":
		// Для строк дополнительная проверка не требуется
	default:
		return errors.New("unsupported data type")
	}
	return nil
}

func ReadTableStructure(filePath string) (header []string, types []string, err error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, nil, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)

	if scanner.Scan() {
		header = strings.Split(scanner.Text(), ",")
	}

	if scanner.Scan() {
		types = strings.Split(scanner.Text(), ",")
	}

	if err := scanner.Err(); err != nil {
		return nil, nil, err
	}

	return header, types, nil
}

func ReadTable(filePath string) ([][]string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		return nil, err
	}

	return records, nil
}

func saveRecordsToFile(filePath string, records [][]string) error {
	tempFilePath := filePath + ".tmp"
	tempFile, err := os.Create(tempFilePath)
	if err != nil {
		return err
	}
	defer tempFile.Close()

	writer := csv.NewWriter(tempFile)
	if err := writer.WriteAll(records); err != nil {
		return err
	}
	writer.Flush()
	if err := writer.Error(); err != nil {
		return err
	}

	return os.Rename(tempFilePath, filePath)
}

func OrderBy(records [][]string, orderByClause string) ([][]string, error) {
	if len(records) < 2 {
		return nil, errors.New("not enough records for sorting")
	}

	columnIndex := -1
	for i, colName := range records[0] {
		if colName == orderByClause {
			columnIndex = i
			break
		}
	}
	if columnIndex == -1 {
		return nil, errors.New("column not found in ORDER BY clause")
	}

	sort.Slice(records[1:], func(i, j int) bool {
		return records[i+1][columnIndex] < records[j+1][columnIndex]
	})

	return records, nil
}

func EvaluateWhere(record []string, header []string, whereClause string) bool {
	parts := strings.Fields(whereClause)
	if len(parts) != 3 {
		return false
	}
	column, operator, value := parts[0], parts[1], parts[2]

	colIndex := -1
	for i, h := range header {
		if h == column {
			colIndex = i
			break
		}
	}
	if colIndex == -1 {
		return false
	}

	return EvaluateCondition(record[colIndex], operator, value)
}

func EvaluateCondition(recordValue, operator, conditionValue string) bool {
	switch operator {
	case "=":
		return recordValue == conditionValue
	case "!=":
		return recordValue != conditionValue
	case ">":
		return recordValue > conditionValue
	case "<":
		return recordValue < conditionValue
	case ">=":
		return recordValue >= conditionValue
	case "<=":
		return recordValue <= conditionValue
	default:
		return false
	}
}
