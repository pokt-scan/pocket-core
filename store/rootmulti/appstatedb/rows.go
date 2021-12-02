package appstatedb

import (
	"database/sql"
)

type AppStateDBRows struct {
	rows    *sql.Rows
	IsEmpty bool
}

type AppStateDBRowEntry struct {

}

func NewAppStateDBRows(rows *sql.Rows) *AppStateDBRows {
	asdbr := &AppStateDBRows{
		rows:    rows,
		IsEmpty: false,
	}

	// If error mark empty so we can
	rowsErr := asdbr.rows.Err()
	if rowsErr != nil {
		asdbr.IsEmpty = true
		rows.Close()
	}

	// Pre-load first value
	preloadSuccess := asdbr.rows.Next()
	if preloadSuccess == false {
		asdbr.IsEmpty = true
		rows.Close()
	}

	return asdbr
}

func (asdbr *AppStateDBRows) Next() {
	success := asdbr.rows.Next()
	if success == false || asdbr.rows.Err() != nil {
		asdbr.IsEmpty = true
		asdbr.rows.Close()
	}
}

func (asdbr *AppStateDBRows) Err() error {
	return asdbr.rows.Err()
}

func (asdbr *AppStateDBRows) Scan(dest ...interface{}) error {
	return asdbr.rows.Scan(dest...)
}

func (asdbr *AppStateDBRows) Close() {
	closeErr := asdbr.rows.Close()
	if closeErr != nil {
		panic(closeErr)
	}
}
