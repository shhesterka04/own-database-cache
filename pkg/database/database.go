package database

import (
	"context"
	"errors"
	"fmt"
	"own-database-cache/pkg/parser"
	"sync"
)

type Transaction struct {
	changes []func() error
}

type Database struct {
	mu           sync.Mutex
	transactions map[*Transaction]bool
	file         string
}

func NewDatabase(file string) *Database {
	return &Database{
		file:         file,
		transactions: make(map[*Transaction]bool),
	}
}

func (d *Database) Begin(ctx context.Context) (*Transaction, error) {
	d.mu.Lock()
	txn := &Transaction{changes: []func() error{}}
	d.transactions[txn] = true
	return txn, nil
}

func (d *Database) Commit(ctx context.Context, txn *Transaction) error {
	if _, ok := d.transactions[txn]; !ok {
		d.mu.Unlock()
		return errors.New("transaction not found")
	}

	for _, change := range txn.changes {
		if err := change(); err != nil {
			d.mu.Unlock()
			return err
		}
	}

	delete(d.transactions, txn)
	d.mu.Unlock()
	return nil
}

func (d *Database) Rollback(ctx context.Context, txn *Transaction) error {
	if _, ok := d.transactions[txn]; !ok {
		d.mu.Unlock()
		return errors.New("transaction not found")
	}

	delete(d.transactions, txn)
	d.mu.Unlock()
	return nil
}

func (d *Database) Exec(ctx context.Context, txn *Transaction, sql string) error {
	if txn == nil {
		return errors.New("transaction is required")
	}

	change := func() error {
		_, err := d.ExecuteQuery(sql)
		return err
	}
	txn.changes = append(txn.changes, change)
	return nil
}

func (d *Database) Query(ctx context.Context, sql string) ([][]string, error) {
	return d.ExecuteQuery(sql)
}

func (d *Database) QueryRow(ctx context.Context, sql string) ([]string, error) {
	results, err := d.Query(ctx, sql)
	if err != nil {
		return nil, err
	}
	if len(results) == 0 {
		return nil, errors.New("no rows returned")
	}
	return results[1], nil
}

func (d *Database) ExecuteQuery(query string) ([][]string, error) {
	parsedQuery, err := parser.ParseSQL(query)
	if err != nil {
		return nil, err
	}

	switch parsedQuery.Operation {
	case "CREATE":
		err = CreateTable(d.file, parsedQuery.TableName, parsedQuery.Columns, parsedQuery.Values[0])
		if err != nil {
			return nil, err
		}
		return nil, nil
	case "INSERT":
		filePath := d.file + parsedQuery.TableName + ".csv"
		header, types, err := ReadTableStructure(filePath)
		if err != nil {
			return nil, err
		}

		for _, values := range parsedQuery.Values {
			for i, value := range values {
				expectedType := types[i]
				if err := checkDataType(value, expectedType); err != nil {
					return nil, fmt.Errorf("error in column %s: %v", header[i], err)
				}
			}
		}

		err = InsertTable(filePath, parsedQuery.Values)
		if err != nil {
			return nil, err
		}
		return nil, nil
	case "DELETE":
		filePath := d.file + parsedQuery.TableName + ".csv"
		header, types, err := ReadTableStructure(filePath)
		if err != nil {
			return nil, err
		}
		err = DeleteTable(filePath, header, types, parsedQuery.WhereClause)
		if err != nil {
			return nil, err
		}
		return nil, nil
	case "SELECT":
		filePath := d.file + parsedQuery.TableName + ".csv"
		return SelectTable(filePath, parsedQuery.Columns, parsedQuery.WhereClause, parsedQuery.OrderByClause)
	case "UPDATE":
		filePath := d.file + parsedQuery.TableName + ".csv"
		header, _, err := ReadTableStructure(filePath)
		if err != nil {
			return nil, err
		}
		records, err := ReadTable(filePath)
		if err != nil {
			return nil, err
		}
		err = UpdateTable(filePath, header, records, parsedQuery.Values[0], parsedQuery.WhereClause)
		if err != nil {
			return nil, err
		}
		return nil, nil
	}

	return nil, errors.New("unsupported operation")
}
