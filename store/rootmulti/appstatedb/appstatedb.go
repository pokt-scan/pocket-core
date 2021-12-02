package appstatedb

import (
	"context"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	dbm "github.com/tendermint/tm-db"
	"strings"
	"sync"
	"time"
)

var dbConnMap = map[string]*sql.DB{}
var txMap = map[string]*sql.Tx{}

type AppStateDB struct {
	dir   string
	table string
	db    *sql.DB
	mu    sync.Mutex
}

func NewAppStateDB(dir string, table string) (*AppStateDB, error) {
	// Find a way to not have to do this
	//fmt.Println(sqlite.SQLITE_OK)
	// Create the db connection
	db, dbError := getDBConn(dir, table) //sql.Open("sqlite3", fmt.Sprintf("file:%s/%s.db?cache=shared&_mutex=full", dir, table))
	if dbError != nil {
		return nil, dbError
	}

	// Create the table if not exists
	_, tableCreationError := db.Exec(fmt.Sprintf(createTableStatement, table))
	if tableCreationError != nil {
		return nil, tableCreationError
	}

	result := &AppStateDB{
		db:    db,
		dir:   dir,
		table: table,
	}

	return result, nil
}

func getTx(dir, table string) (*sql.Tx, error) {
	var result = txMap[table]
	if result == nil {
		db, dbErr := getDBConn(dir, table) //sql.Open("sqlite3", fmt.Sprintf("file:%s/%s.db?cache=shared&_mutex=full", dir, table))
		if dbErr != nil {
			return nil, dbErr
		}

		// Figure out what to do with the cancel function
		ctx, _ := context.WithCancel(context.Background())
		// TODO: understand txoptions
		// Get db
		tx, beginTxError := db.BeginTx(ctx, nil)
		if beginTxError != nil {
			return nil, beginTxError
		}
		txMap[table] = tx
		return tx, nil
	}
	return result, nil
}

func getDBConn(dir, table string) (*sql.DB, error) {
	var result = dbConnMap[table]
	if result == nil {
		dbConn, dbConnErr := sql.Open("sqlite3", fmt.Sprintf("file:%s/%s.db?cache=shared", dir, table))
		if dbConnErr != nil {
			return nil, dbConnErr
		}
		dbConnMap[table] = dbConn
		return dbConn, nil
	}
	return result, nil
}

// Has interface
func (asdb *AppStateDB) Has(height int64, table string, key []byte) (bool, error) {
	defer timeTrack(time.Now(), "Has")
	//asdb.mu.Lock()
	////defer asdb.mu.Unlock()

	// Prepare the query
	//queryStmt, queryStmtErr := asdb.db.Prepare(fmt.Sprintf(GetQuery, table, table, table, table, table, table, table))
	//if queryStmtErr != nil {
	//	return false, queryStmtErr
	//}
	//defer queryStmt.Close()
	//
	//// Execute the query
	//var result string
	//queryError := queryStmt.QueryRow(key, height, height).Scan(&result)
	//// Check for empty result
	//if queryError == sql.ErrNoRows {
	//	return false, nil
	//}
	//// Check for other error types
	//if queryError != nil {
	//	return false, queryError
	//}
	panic("FALSO")
	return false, nil
}

func (asdb *AppStateDB) HasMutable(height int64, table string, key []byte) (bool, error) {
	defer timeTrack(time.Now(), fmt.Sprintf("Has Mutable"))
	//asdb.mu.Lock()
	//defer asdb.mu.Unlock()

	//// Get the tx
	//tx, txErr := getTx(asdb.dir, asdb.table)
	//if txErr != nil {
	//	return false, txErr
	//}
	//
	//// Prepare the query
	//queryStmt, queryStmtErr := tx.Prepare(fmt.Sprintf(GetQuery, table, table, table, table, table, table, table))
	//if queryStmtErr != nil {
	//	return false, queryStmtErr
	//}
	//defer queryStmt.Close()
	//
	//// Execute the query
	//var result string
	//queryError := queryStmt.QueryRow(key, height, height).Scan(&result)
	//// Check for empty result
	//if queryError == sql.ErrNoRows {
	//	return false, nil
	//}
	//// Check for other error types
	//if queryError != nil {
	//	return false, queryError
	//}
	panic("FALSO")
	return false, nil
}

// Get interface
//func (asdb *AppStateDB) Get(height int64, table string, key []byte) ([]byte, error) {
//	defer timeTrack(time.Now(), "Get")
//	//asdb.mu.Lock()
//	//defer asdb.mu.Unlock()
//
//	// Prepare the query
//	hexKey := strings.ToUpper(hex.EncodeToString(key))
//	queryStmt, queryStmtErr := asdb.db.Prepare(fmt.Sprintf(GetQuery, table, table, table, hexKey, table, table, table, table))
//	if queryStmtErr != nil {
//		return nil, queryStmtErr
//	}
//	defer queryStmt.Close()
//
//	// Execute the query
//	var result string
//	queryError := queryStmt.QueryRow(height, height).Scan(&result)
//	// Check for empty result
//	if queryError == sql.ErrNoRows {
//		return nil, nil
//	}
//	// Check for other error types
//	if queryError != nil {
//		return nil, queryError
//	}
//	return []byte(result), nil
//}

func logAndReturn(log string) string {
	// fmt.Println(strings.ReplaceAll(log, "\n", " "))
	return log
}

func (asdb *AppStateDB) GetMutable(height int64, table string, key []byte) ([]byte, error) {
	defer timeTrack(time.Now(), fmt.Sprintf("Get Mutable"))
	//asdb.mu.Lock()
	//defer asdb.mu.Unlock()

	// Get the tx
	tx, txErr := getTx(asdb.dir, asdb.table)
	if txErr != nil {
		return nil, txErr
	}

	//queryStmtStr := //
	//fmt.Println(strings.ReplaceAll(queryStmtStr, "\n", " "))
	//queryStmt, queryStmtErr := tx.Prepare(logAndReturn(fmt.Sprintf(GetQuery, table, table, table, hexKey, height, table, table, table, table, height)))
	//if queryStmtErr != nil {
	//	return nil, queryStmtErr
	//}
	//defer queryStmt.Close()

	// Prepare the query
	hexKey := strings.ToUpper(hex.EncodeToString(key))

	// Execute the query
	var result string
	queryError := tx.QueryRow(logAndReturn(fmt.Sprintf(GetQuery, table, hexKey, height, height))).Scan(&result)
	// Check for empty result
	if queryError == sql.ErrNoRows {
		return nil, nil
	}
	// Check for other error types
	if queryError != nil {
		return nil, queryError
	}
	return []byte(result), nil
}

// Set interface
//func (asdb *AppStateDB) Set(height int64, table string, key []byte, value []byte) error {
//	defer timeTrack(time.Now(), "Set")
//	//asdb.mu.Lock()
//	//defer asdb.mu.Unlock()
//
//	// Execute the insert statement
//	result, insertExecErr := asdb.db.Exec(fmt.Sprintf(InsertStatement, table, table, table, table, table, table, table, table), height, key, value, value, key, height, height)
//	if insertExecErr != nil {
//		return insertExecErr
//	}
//
//	rowsAff, rowsAffErr := result.RowsAffected()
//	if rowsAffErr != nil {
//		panic(rowsAffErr.Error())
//	}
//
//	if rowsAff > 1 {
//		return errors.New(fmt.Sprintf("Affected rows on set > 1 %d", rowsAff))
//	}
//
//	// Success!
//	return nil
//}

func (asdb *AppStateDB) SetMutable(height int64, table string, key, value []byte) error {
	defer timeTrack(time.Now(), fmt.Sprintf("Set Mutable"))
	//asdb.mu.Lock()
	//defer asdb.mu.Unlock()

	// Get the tx
	tx, txErr := getTx(asdb.dir, asdb.table)
	if txErr != nil {
		return txErr
	}

	// Execute the insert statement
	hexKey := strings.ToUpper(hex.EncodeToString(key))
	hexValue := strings.ToUpper(hex.EncodeToString(value))
	result, insertExecErr := tx.Exec(logAndReturn(fmt.Sprintf(InsertStatement, table, height, hexKey, hexValue)))
	if insertExecErr != nil {
		return insertExecErr
	}

	rowsAff, rowsAffErr := result.RowsAffected()
	if rowsAffErr != nil {
		panic(rowsAffErr.Error())
	}

	if rowsAff != 1 {
		return errors.New(fmt.Sprintf("Affected rows on set > 1 %d", rowsAff))
	}

	// Success!
	return nil
}

// Delete interface
//func (asdb *AppStateDB) Delete(height int64, table string, key []byte) error {
//	defer timeTrack(time.Now(), "Delete")
//	//asdb.mu.Lock()
//	//defer asdb.mu.Unlock()
//
//	// Prepare the delete statement
//	hexKey := strings.ToUpper(hex.EncodeToString(key))
//	deleteStmt, deleteStmtErr := asdb.db.Prepare(logAndReturn(fmt.Sprintf(DeleteStatement, table, height, table, table, table, table, hexKey, height, table, table, table, table, height)))
//	if deleteStmtErr != nil {
//		return deleteStmtErr
//	}
//	defer deleteStmt.Close()
//
//	// Execute the delete statement
//	delResult, deleteExecErr := deleteStmt.Exec()
//	if deleteExecErr != nil {
//		return deleteExecErr
//	}
//
//	delResultRowsAffected, delResultRowsAffectedErr := delResult.RowsAffected()
//	if delResultRowsAffectedErr != nil {
//		return delResultRowsAffectedErr
//	}
//
//	if delResultRowsAffected > 1 {
//		return errors.New(fmt.Sprintf("Affected rows on delete != 1 %d", delResultRowsAffected))
//	}
//
//	// Success!
//	return nil
//}

func (asdb *AppStateDB) DeleteMutable(height int64, table string, key []byte) error {
	defer timeTrack(time.Now(), fmt.Sprintf("Delete Mutable"))
	//asdb.mu.Lock()
	//defer asdb.mu.Unlock()

	tx, txErr := getTx(asdb.dir, asdb.table)
	if txErr != nil {
		return txErr
	}

	// Prepare the delete statement
	// hexKey := strings.ToUpper(hex.EncodeToString(key))
	//deleteStmt, deleteStmtErr := tx.Prepare(logAndReturn(fmt.Sprintf(DeleteStatement, table, height, table, table, table, table, hexKey, height, table, table, table, table, height)))
	//if deleteStmtErr != nil {
	//	return deleteStmtErr
	//}
	//defer deleteStmt.Close()

	// Prepare the delete statement
	hexKey := strings.ToUpper(hex.EncodeToString(key))
	// Execute the delete statement
	delResult, deleteExecErr := tx.Exec(logAndReturn(fmt.Sprintf(DeleteStatement, table, height, table, hexKey, height, height)))
	if deleteExecErr != nil {
		return deleteExecErr
	}

	delResultRowsAffected, delResultRowsAffectedErr := delResult.RowsAffected()
	if delResultRowsAffectedErr != nil {
		return delResultRowsAffectedErr
	}

	if delResultRowsAffected != 1 {
		return errors.New(fmt.Sprintf("Affected rows on delete != 1 %d", delResultRowsAffected))
	}

	// Success!
	return nil
}

type iteratorOrder int64

const (
	Ascending iteratorOrder = iota
	Descending
)

func (itor iteratorOrder) String() string {
	switch itor {
	case Ascending:
		return "ASC"
	case Descending:
		return "DESC"
	}
	return "ASC"
}

func (asdb *AppStateDB) iteratorMutableSorted(height int64, table string, start, end []byte, order iteratorOrder) (dbm.Iterator, error) {
	defer timeTrack(time.Now(), fmt.Sprintf("Iterator Mutable with order %s", order.String()))
	//asdb.mu.Lock()
	//defer asdb.mu.Unlock()

	tx, txErr := getTx(asdb.dir, asdb.table)
	if txErr != nil {
		return nil, txErr
	}

	// Prepare the query
	var iteratorQueryStr string
	if start == nil && end == nil {
		iteratorQueryStr = fmt.Sprintf(IteratorAllQuery, table, height, height, order.String())
	} else {
		hexStart := strings.ToUpper(hex.EncodeToString(start))
		hexEnd := strings.ToUpper(hex.EncodeToString(end))
		iteratorQueryStr = fmt.Sprintf(IteratorQuery, table, height, hexStart, hexEnd, height, order.String())
	}

	//queryStmt, queryStmtErr := tx.Prepare(logAndReturn(iteratorQueryStr))
	//if queryStmtErr != nil {
	//	return nil, queryStmtErr
	//}
	//defer queryStmt.Close()

	// Execute the query
	rows, queryError := tx.Query(logAndReturn(iteratorQueryStr))
	if queryError != nil {
		return nil, queryError
	}

	// Create and return the iterator
	return NewAppStateDBIterator(start, end, rows), nil
}

//func (asdb *AppStateDB) iteratorSorted(height int64, table string, start, end []byte, order iteratorOrder) (dbm.Iterator, error) {
//	defer timeTrack(time.Now(), "iteratorSorted Immutable")
//	//asdb.mu.Lock()
//	//defer asdb.mu.Unlock()
//
//	// Prepare the query
//	var iteratorQueryStr string
//	if start == nil {
//		iteratorQueryStr = fmt.Sprintf(IteratorAllQuery, table, table, table, table, table, table, table, table, table, order.String())
//	} else {
//		hexStart := hex.EncodeToString(start)
//		iteratorQueryStr = fmt.Sprintf(IteratorQuery, table, table, table, table, table, hexStart, table, table, table, table, order.String())
//	}
//
//	queryStmt, queryStmtErr := asdb.db.Prepare(iteratorQueryStr)
//	if queryStmtErr != nil {
//		return nil, queryStmtErr
//	}
//	defer queryStmt.Close()
//
//	// Execute the query
//	rows, queryError := queryStmt.Query(height, height)
//	if queryError != nil {
//		return nil, queryError
//	}
//
//	// Create and return the iterator
//	return NewAppStateDBIterator(start, end, rows), nil
//}

//func (asdb *AppStateDB) Iterator(height int64, table string, start, end []byte) (dbm.Iterator, error) {
//	return asdb.iteratorSorted(height, table, start, end, Ascending)
//}

func (asdb *AppStateDB) IteratorMutable(height int64, table string, start, end []byte) (dbm.Iterator, error) {
	return asdb.iteratorMutableSorted(height, table, start, end, Ascending)
}

//func (asdb *AppStateDB) ReverseIterator(height int64, table string, start, end []byte) (dbm.Iterator, error) {
//	return asdb.iteratorSorted(height, table, start, end, Descending)
//}

func (asdb *AppStateDB) ReverseIteratorMutable(height int64, table string, start, end []byte) (dbm.Iterator, error) {
	return asdb.iteratorMutableSorted(height, table, start, end, Descending)
}

func (asdb *AppStateDB) CommitMutable() error {
	defer timeTrack(time.Now(), "CommitMutable")
	//asdb.mu.Lock()
	//defer asdb.mu.Unlock()

	tx, txErr := getTx(asdb.dir, asdb.table)
	if txErr != nil {
		return txErr
	}

	commitErr := tx.Commit()
	if commitErr != nil {
		return commitErr
	}

	txMap[asdb.table] = nil
	return nil
}
