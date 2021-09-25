package types

import (
	"bytes"
	"io"
)

var _ KVStore = PrefixStore{}

// PrefixStore is similar with tendermint/tendermint/libs/db/prefix_db
// both gives access only to the limited subset of the store
// for convinience or safety
type PrefixStore struct {
	parent KVStore
	prefix []byte
}

func NewPrefixStore(parent KVStore, prefix []byte) PrefixStore {
	return PrefixStore{
		parent: parent,
		prefix: prefix,
	}
}

func cloneAppend(bz []byte, tail []byte) (res []byte) {
	res = make([]byte, len(bz)+len(tail))
	copy(res, bz)
	copy(res[len(bz):], tail)
	return
}

func (s PrefixStore) key(key []byte) (res []byte) {
	if key == nil {
		panic("nil key on PrefixStore")
	}
	res = cloneAppend(s.prefix, key)
	return
}

// Implements PrefixStore
func (s PrefixStore) GetStoreType() StoreType {
	return s.parent.GetStoreType()
}

// Implements CacheWrap
func (s PrefixStore) CacheWrap() CacheWrap {
	panic("")
}

// CacheWrapWithTrace implements the KVStore interface.
func (s PrefixStore) CacheWrapWithTrace(w io.Writer, tc TraceContext) CacheWrap {
	panic("")
}

// Implements KVStore
func (s PrefixStore) Get(key []byte) ([]byte, error) {
	res, _ := s.parent.Get(s.key(key))
	return res, nil
}

// Implements KVStore
func (s PrefixStore) Has(key []byte) (bool, error) {
	return s.parent.Has(s.key(key))
}

// Implements KVStore
func (s PrefixStore) Set(key, value []byte) error {
	AssertValidKey(key)
	AssertValidValue(value)
	return s.parent.Set(s.key(key), value)
}

// Implements KVStore
func (s PrefixStore) Delete(key []byte) error {
	return s.parent.Delete(s.key(key))
}

// Implements KVStore
// Check https://github.com/tendermint/tendermint/blob/master/libs/db/prefix_db.go#L106
func (s PrefixStore) Iterator(start, end []byte) (Iterator, error) {
	newstart := cloneAppend(s.prefix, start)

	var newend []byte
	if end == nil {
		newend = cpIncr(s.prefix)
	} else {
		newend = cloneAppend(s.prefix, end)
	}

	iter, err := s.parent.Iterator(newstart, newend)

	return newPrefixIterator(s.prefix, start, end, iter), err
}

// Implements KVStore
// Check https://github.com/tendermint/tendermint/blob/master/libs/db/prefix_db.go#L129
func (s PrefixStore) ReverseIterator(start, end []byte) (Iterator, error) {
	newstart := cloneAppend(s.prefix, start)

	var newend []byte
	if end == nil {
		newend = cpIncr(s.prefix)
	} else {
		newend = cloneAppend(s.prefix, end)
	}

	iter, err := s.parent.ReverseIterator(newstart, newend)

	return newPrefixIterator(s.prefix, start, end, iter), err
}

var _ Iterator = (*prefixIterator)(nil)

type prefixIterator struct {
	prefix     []byte
	start, end []byte
	iter       Iterator
	valid      bool
}

func (iter *prefixIterator) Error() error {
	panic("implement me")
}

func newPrefixIterator(prefix, start, end []byte, parent Iterator) *prefixIterator {
	return &prefixIterator{
		prefix: prefix,
		start:  start,
		end:    end,
		iter:   parent,
		valid:  parent.Valid() && bytes.HasPrefix(parent.Key(), prefix),
	}
}

// Implements Iterator
func (iter *prefixIterator) Domain() ([]byte, []byte) {
	return iter.start, iter.end
}

// Implements Iterator
func (iter *prefixIterator) Valid() bool {
	return iter.valid && iter.iter.Valid()
}

// Implements Iterator
func (iter *prefixIterator) Next() {
	if !iter.valid {
		panic("prefixIterator invalid, cannot call Next()")
	}
	iter.iter.Next()
	if !iter.iter.Valid() || !bytes.HasPrefix(iter.iter.Key(), iter.prefix) {
		iter.valid = false
	}
}

// Implements Iterator
func (iter *prefixIterator) Key() (key []byte) {
	if !iter.valid {
		panic("prefixIterator invalid, cannot call Key()")
	}
	key = iter.iter.Key()
	key = stripPrefix(key, iter.prefix)
	return
}

// Implements Iterator
func (iter *prefixIterator) Value() []byte {
	if !iter.valid {
		panic("prefixIterator invalid, cannot call Value()")
	}
	return iter.iter.Value()
}

// Implements Iterator
func (iter *prefixIterator) Close() {
	iter.iter.Close()
}

// copied from github.com/tendermint/tendermint/libs/db/prefix_db.go
func stripPrefix(key []byte, prefix []byte) []byte {
	if len(key) < len(prefix) || !bytes.Equal(key[:len(prefix)], prefix) {
		panic("should not happen")
	}
	return key[len(prefix):]
}

// wrapping types.PrefixEndBytes
func cpIncr(bz []byte) []byte {
	return PrefixEndBytes(bz)
}
