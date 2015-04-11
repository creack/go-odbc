// Copyright (c) 2012, Wei guangjing <vcc.163@gmail.com>. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package driver

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"

	"github.com/creack/godbc"
)

func init() {
	d := &Driver{}
	sql.Register("odbc", d)
}

// Driver wraps the odbc driver for the database/sql package
type Driver struct{}

// Open establish the odbc connection
func (d *Driver) Open(dsn string) (driver.Conn, error) {
	c, err := godbc.Connect(dsn)
	if err != nil {
		return nil, err
	}
	return &conn{c: c}, nil
}

// Close terminates the session
func (d *Driver) Close() error { return nil }

type conn struct {
	c *godbc.Connection
	t *tx
}

func (c *conn) Prepare(query string) (driver.Stmt, error) {
	st, err := c.c.Prepare(query)
	if err != nil {
		return nil, err
	}
	return &stmt{st: st}, nil
}

func (c *conn) Begin() (driver.Tx, error) {
	if err := c.c.AutoCommit(false); err != nil {
		return nil, err
	}
	return &tx{c: c}, nil
}

func (c *conn) Close() error {
	if c.c != nil {
		return c.c.Close()
	}
	return nil
}

type tx struct {
	c *conn
}

func (t *tx) Commit() error {
	return t.c.c.Commit()
}

func (t *tx) Rollback() error {
	return t.c.c.Rollback()
}

type stmt struct {
	st *godbc.Statement
}

func (s *stmt) Exec(args []driver.Value) (driver.Result, error) {
	if err := s.st.Execute2(args); err != nil {
		return nil, err
	}
	rowsAffected, err := s.st.RowsAffected()
	return &result{rowsAffected: int64(rowsAffected)}, err
}

func (s *stmt) NumInput() int {
	return s.st.NumParams()
}

func (s *stmt) Query(args []driver.Value) (driver.Rows, error) {
	if err := s.st.Execute2(args); err != nil {
		return nil, err
	}
	return &rows{s: s}, nil
}

func (s *stmt) Close() error {
	s.st.Close()
	return nil
}

type result struct {
	rowsAffected int64
}

func (r *result) LastInsertId() (int64, error) { return 0, errors.New("not supported") }

func (r *result) RowsAffected() (int64, error) { return r.rowsAffected, nil }

type rows struct {
	s *stmt
}

func (r *rows) Columns() []string {
	c, err := r.s.st.NumFields()
	if err != nil {
		return nil
	}
	columns := make([]string, c)
	for i := range columns {
		f, err := r.s.st.FieldMetadata(i + 1)
		if err != nil {
			return nil
		}
		columns[i] = f.Name
	}
	return columns
}

func (r *rows) Close() error {
	return r.s.Close()
}

func (r *rows) Next(dest []driver.Value) error {
	eof, err := r.s.st.FetchOne2(dest)
	if err != nil {
		return err
	}
	if eof {
		return io.EOF
	}
	return nil
}
