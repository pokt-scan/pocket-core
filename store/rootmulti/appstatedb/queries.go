package appstatedb

//language=SQL
const GetQuery = `
SELECT value
  FROM %s
 WHERE key = X'%s' AND
       height <= %d AND
       deleted_at > %d
ORDER BY height DESC
LIMIT 1
`

//language=SQL
const InsertStatement = `
INSERT OR REPLACE INTO %s(height, key, value) VALUES(%d, X'%s', X'%s')
`

//language=SQL
const DeleteStatement = `
UPDATE %s
   SET deleted_at = %d
 WHERE (key, height) IN (
 SELECT key, height
  FROM %s
 WHERE key = X'%s' AND
       height <= %d AND
       deleted_at > %d
ORDER BY height DESC
LIMIT 1
 )
`

//language=SQL
const IteratorQuery = `
SELECT key, value
  FROM %s
 WHERE height <= %d AND
       HEX(key) >= '%s' AND
       HEX(key) < '%s' AND
       deleted_at > %d
GROUP BY key
HAVING height = MAX(height)
ORDER BY key %s
`

//language=SQL
const IteratorAllQuery = `
SELECT key, value
  FROM %s
 WHERE height <= %d AND
       deleted_at > %d
GROUP BY key
HAVING height = MAX(height)
ORDER BY key %s
`

const createTableStatement = `
CREATE TABLE IF NOT EXISTS %s (height INTEGER NOT NULL, key BLOB, value BLOB, deleted_at INTEGER DEFAULT 9223372036854775807, PRIMARY KEY(height, key));
`
