package appstatedb

import (
	"context"
	"database/sql"
	"fmt"
	dbm "github.com/tendermint/tm-db"
	"time"
)

// Mutable App State DB
type MutableAppStateDB struct {
	cancel   context.CancelFunc
	tx       *sql.Tx
	isTxDone bool
	ctx      context.Context
}

func NewMutableAppStateDB(db *sql.DB) (*MutableAppStateDB, error) {
	ctx, cancel := context.WithCancel(context.Background())
	// TODO: understand txoptions
	// Get db
	tx, beginTxError := db.BeginTx(ctx, nil)
	if beginTxError != nil {
		return nil, beginTxError
	}
	return &MutableAppStateDB{
		cancel:   cancel,
		tx:       tx,
		isTxDone: false,
		ctx:      ctx,
	}, nil
}

func (masdb *MutableAppStateDB) Has(height int64, table string, key string) (bool, error) {
	// Prepare the query
	queryStmt, queryStmtErr := masdb.tx.Prepare(fmt.Sprintf(GetQuery, table))
	if queryStmtErr != nil {
		return false, queryStmtErr
	}
	defer queryStmt.Close()

	// Execute the query
	var result string
	queryError := queryStmt.QueryRow(key, height, height).Scan(&result)
	if queryError != nil {
		return false, queryError
	}
	return true, nil
}

func (masdb *MutableAppStateDB) Get(height int64, table string, key string) ([]byte, error) {
	// Prepare the query
	queryStr := fmt.Sprintf(GetQuery, table)
	queryStmt, queryStmtErr := masdb.tx.Prepare(queryStr)
	if queryStmtErr != nil {
		return nil, queryStmtErr
	}
	defer queryStmt.Close()

	// Execute the query
	var result string
	queryError := queryStmt.QueryRow(key, height, height).Scan(&result)
	if queryError != nil {
		// Return empty bytes if no records are found
		if queryError == sql.ErrNoRows {
			return nil, nil
		}
		return nil, queryError
	}
	return []byte(result), nil
}

func (masdb *MutableAppStateDB) Set(height int64, table string, key string, value string) error {
	if table == "params" {
		fmt.Println("This is params")
	}

	// Prepare the insert statement
	insertStmt, insertStmtErr := masdb.tx.PrepareContext(masdb.ctx, fmt.Sprintf(InsertStatement, table, table))
	if insertStmtErr != nil {
		return insertStmtErr
	}
	defer insertStmt.Close()

	// Execute the insert statement
	insertResult, insertExecErr := insertStmt.ExecContext(masdb.ctx, height, key, value, value, key, height, height)
	rowsAffected, rowsAffectedErr := insertResult.RowsAffected()
	if rowsAffectedErr != nil {
		fmt.Println(rowsAffectedErr)
	}
	fmt.Println(fmt.Sprintf("Rows affected by insert: %d", rowsAffected))
	if insertExecErr != nil {
		fmt.Println(insertExecErr)
		return insertExecErr
	}

	// Success!
	return nil
}

func (masdb *MutableAppStateDB) Delete(height int64, table string, key string) error {
	// Prepare the delete statement
	deleteStmt, deleteStmtErr := masdb.tx.PrepareContext(masdb.ctx, fmt.Sprintf(DeleteStatement, table, table))
	if deleteStmtErr != nil {
		return deleteStmtErr
	}
	defer deleteStmt.Close()

	// Execute the delete statement
	_, deleteExecErr := deleteStmt.ExecContext(masdb.ctx, key, height, height)
	if deleteExecErr != nil {
		return deleteExecErr
	}

	// Success!
	return nil
}

func (masdb *MutableAppStateDB) iteratorSorted(height int64, table string, start, end []byte, order iteratorOrder) (dbm.Iterator, error) {
	defer timeTrack(time.Now(), "iteratorSorted mutable")
	// Prepare the query
	// Prepare the query
	var iteratorQueryStr string
	if start == nil {
		iteratorQueryStr = fmt.Sprintf(IteratorAllQuery, table, table, table, table, order.String())
	} else {
		iteratorQueryStr = fmt.Sprintf(IteratorQuery, table, table, table, table, string(start), order.String())
	}
	queryStmt, queryStmtErr := masdb.tx.Prepare(iteratorQueryStr)
	if queryStmtErr != nil {
		return nil, queryStmtErr
	}
	defer queryStmt.Close()

	// Execute the query
	rows, queryError := queryStmt.Query(height, height)
	if queryError != nil {
		return nil, queryError
	}

	// Create and return the iterator
	return NewAppStateDBIterator(start, end, rows), nil
}

func (masdb *MutableAppStateDB) Iterator(height int64, table string, start, end []byte) (dbm.Iterator, error) {
	return masdb.iteratorSorted(height, table, start, end, Ascending)
}

func (masdb *MutableAppStateDB) ReverseIterator(height int64, table string, start, end []byte) (dbm.Iterator, error) {
	return masdb.iteratorSorted(height, table, start, end, Descending)
}

func (masdb *MutableAppStateDB) Close() {
	masdb.isTxDone = true
	masdb.cancel()
}

func (masdb *MutableAppStateDB) Commit() error {
	commitErr := masdb.tx.Commit()
	masdb.isTxDone = true
	return commitErr
}
