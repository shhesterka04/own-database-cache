package database

import (
	"encoding/csv"
	"errors"
	"fmt"
	"os"
	"strings"
)

func CreateTable(dbFile, tableName string, columns, dataTypes []string) error {
	if len(columns) != len(dataTypes) {
		return errors.New("number of columns and data types must match")
	}

	if err := os.MkdirAll(dbFile, 0755); err != nil {
		return fmt.Errorf("failed to create database directory: %w", err)
	}

	filePath := dbFile + tableName + ".csv"
	if _, err := os.Stat(filePath); err == nil {
		return errors.New("table already exists")
	}

	file, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("failed to create table file: %w", err)
	}
	defer file.Close()

	_, err = file.WriteString(strings.Join(columns, ",") + "\n")
	if err != nil {
		return err
	}

	_, err = file.WriteString(strings.Join(dataTypes, ",") + "\n")
	return err
}

func UpdateTable(filePath string, header []string, records [][]string, setClauses []string, whereClause string) error {
	for i := 1; i < len(records); i++ {
		if whereClause == "" || EvaluateWhere(records[i], header, whereClause) {
			for _, set := range setClauses {
				parts := strings.SplitN(set, "=", 2)
				if len(parts) != 2 {
					return errors.New("неверный SET-клауз")
				}
				column := strings.TrimSpace(parts[0])
				value := strings.TrimSpace(parts[1])
				value = strings.Trim(value, "'")

				for j, h := range header {
					if h == column {
						records[i][j] = value
						break
					}
				}
			}
		}
	}

	return saveRecordsToFile(filePath, records)
}

func DeleteTable(filePath string, header, types []string, whereClause string) error {
	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		return err
	}

	var remainingRecords [][]string
	remainingRecords = append(remainingRecords, header)
	remainingRecords = append(remainingRecords, types)

	for _, record := range records[2:] {
		if whereClause == "" || !EvaluateWhere(record, header, whereClause) {
			remainingRecords = append(remainingRecords, record)
		}
	}

	return UpdateTable(filePath, header, remainingRecords, []string{}, "")
}

func InsertTable(filePath string, values [][]string) error {
	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	for _, value := range values {
		_, err = file.WriteString(strings.Join(value, ",") + "\n")
		if err != nil {
			return err
		}
	}

	return nil
}

func SelectTable(filePath string, columns []string, whereClause, orderByClause string) ([][]string, error) {
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

	if len(records) < 3 {
		return nil, errors.New("no records found")
	}

	header := records[0]
	var resultRecords [][]string
	resultRecords = append(resultRecords, columns)
	for _, record := range records[2:] {
		if whereClause != "" && !EvaluateWhere(record, header, whereClause) {
			continue
		}
		var resultRecord []string
		for _, colName := range columns {
			for j, h := range header {
				if h == colName {
					resultRecord = append(resultRecord, record[j])
					break
				}
			}
		}
		resultRecords = append(resultRecords, resultRecord)
	}

	if orderByClause != "" {
		resultRecords, err = OrderBy(resultRecords, orderByClause)
	}

	return resultRecords, nil
}
