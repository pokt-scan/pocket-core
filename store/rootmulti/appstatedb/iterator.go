package appstatedb

import (
	"database/sql"
	"errors"
	dbm "github.com/tendermint/tm-db"
)

var _ dbm.Iterator = Iterator{}

// Is 'height/storeKey' aware
type Iterator struct {
	start   []byte
	end     []byte
	entries IteratorEntries
	currIdx *int
}

type IteratorEntry struct {
	key   []byte
	value []byte
}

type IteratorEntries = []IteratorEntry

func NewAppStateDBIterator(start, end []byte, rows *sql.Rows) Iterator {
	// Create and return the iterator
	iterator := Iterator{
		start:   start,
		end:     end,
		currIdx: new(int),
	}

	// Process the entries
	defer rows.Close()
	iterator.entries = rowsToEntries(rows)

	return iterator
}

func rowsToEntries(rows *sql.Rows) IteratorEntries {
	entries := IteratorEntries{}
	for rows.Next() {
		var (
			key   string
			value string
		)
		if err := rows.Scan(&key, &value); err != nil {
			//log.Fatal(err)
			panic(err)
		}
		entries = append(entries, IteratorEntry{
			key:   []byte(key),
			value: []byte(value),
		})
	}
	return entries
}

func (s Iterator) Length() int {
	return len(s.entries)
}

func (s Iterator) Valid() bool {

	if s.Length() == 0 {
		return false
	}

	if *s.currIdx >= s.Length() {
		return false
	}

	return true
	//// Check rows error
	//iteratorErr := s.rows.Err()
	//if iteratorErr != nil {
	//	defer s.rows.Close()
	//	return false
	//}
	//
	//// Check if empty
	//if s.rows.IsEmpty {
	//	defer s.rows.Close()
	//	return false
	//}
	//
	//// Is valid
	//return true
}

func (s Iterator) Next() {
	*s.currIdx = *s.currIdx + 1
	//s.rows.Next()
}

//func (s Iterator) currPosKeyValue() (key, value []byte) {
//	valScanErr := s.rows.Scan(&key, &value)
//	if valScanErr != nil {
//		panic(valScanErr)
//	}
//	return key, value
//}

func (s Iterator) Key() (key []byte) {
	//key, _ = s.currPosKeyValue()
	return s.entries[*s.currIdx].key
}

func (s Iterator) Value() (value []byte) {
	return s.entries[*s.currIdx].value
}

func (s Iterator) Error() error {
	//return s.rows.Err()
	if s.Length() == 0 {
		return errors.New("Empty iterator")
	}

	if *s.currIdx >= s.Length() {
		return errors.New("Iterator traversed")
	}

	return nil
}

func (s Iterator) Close() {
	*s.currIdx = s.Length()
}

func (s Iterator) Domain() (start []byte, end []byte) {
	return s.start, s.end
}
