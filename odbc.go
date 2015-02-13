// Copyright (c) 2011, Wei guangjing <vcc.163@gmail.com>. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package odbc

/*
#cgo darwin LDFLAGS: -lodbc
#cgo freebsd LDFLAGS: -lodbc
#cgo linux LDFLAGS: -lodbc
#cgo windows LDFLAGS: -lodbc32

#include <stdio.h>
#include <stdlib.h>

#ifdef __MINGW32__
  #include <windef.h>
#else
  typedef void* HANDLE;
#endif

#include <sql.h>
#include <sqlext.h>
#include <sqltypes.h>

SQLRETURN _SQLColAttribute (
	SQLHSTMT        StatementHandle,
	SQLUSMALLINT    ColumnNumber,
	SQLUSMALLINT    FieldIdentifier,
	SQLPOINTER      CharacterAttributePtr,
	SQLSMALLINT     BufferLength,
	SQLSMALLINT *   StringLengthPtr,
	void *        NumericAttributePtr) {
		return SQLColAttribute(StatementHandle,
			ColumnNumber,
			FieldIdentifier,
			CharacterAttributePtr,
			BufferLength,
			StringLengthPtr,
			NumericAttributePtr);
}

*/
import "C"
import (
	"database/sql/driver"
	"fmt"
	"reflect"
	"time"
	"unsafe"
)

func init() {
	if err := initEnv(); err != nil {
		panic("odbc init env error!" + err.Error())
	}
}

const (
	bufferSize    = 10 * 1024
	infoBufferLen = 256
)

var (
	Genv C.SQLHANDLE
)

type Connection struct {
	Dbc       C.SQLHANDLE
	connected bool
}

type Statement struct {
	executed   bool
	prepared   bool
	scrollable bool

	handle C.SQLHANDLE
}

type Error struct {
	SQLState     string
	NativeError  int
	ErrorMessage string
}

func (e *Error) Error() string {
	if e != nil {
		return e.SQLState + " " + e.ErrorMessage
	}
	return ""
}
func (e *Error) String() string { return e.Error() }

func initEnv() error {
	if ret := C.SQLAllocHandle(C.SQL_HANDLE_ENV, nil, &Genv); !Success(ret) {
		return FormatError(C.SQL_HANDLE_ENV, Genv)
	}
	if ret := C.SQLSetEnvAttr(C.SQLHENV(Genv), C.SQL_ATTR_ODBC_VERSION, C.SQLPOINTER(unsafe.Pointer(uintptr(C.SQL_OV_ODBC3))), C.SQLINTEGER(0)); !Success(ret) {
		return FormatError(C.SQL_HANDLE_ENV, Genv)
	}
	return nil
}

func Connect(dsn string, params ...interface{}) (*Connection, error) {
	var h C.SQLHANDLE

	if ret := C.SQLAllocHandle(C.SQL_HANDLE_DBC, Genv, &h); !Success(ret) {
		return nil, FormatError(C.SQL_HANDLE_DBC, h)
	}

	var (
		stringLength2       C.SQLSMALLINT
		outBuf              = make([]byte, bufferSize*2)
		outConnectionString = (*C.SQLWCHAR)(unsafe.Pointer(&outBuf[0]))
	)
	if ret := C.SQLDriverConnectW(C.SQLHDBC(h),
		nil,
		(*C.SQLWCHAR)(unsafe.Pointer(StringToUTF16Ptr(dsn))),
		C.SQL_NTS,
		outConnectionString,
		bufferSize,
		&stringLength2,
		C.SQL_DRIVER_NOPROMPT); !Success(ret) {
		return nil, FormatError(C.SQL_HANDLE_DBC, h)
	}
	return &Connection{Dbc: h, connected: true}, nil
}

func (conn *Connection) ExecDirect(sql string) (*Statement, error) {
	stmt, err := conn.newStmt()
	if err != nil {
		return nil, err
	}
	wsql := StringToUTF16Ptr(sql)

	if ret := C.SQLExecDirectW(
		C.SQLHSTMT(stmt.handle),
		(*C.SQLWCHAR)(unsafe.Pointer(wsql)),
		C.SQL_NTS); !Success(ret) {
		err := FormatError(C.SQL_HANDLE_STMT, stmt.handle)
		stmt.Close()
		return nil, err
	}
	stmt.executed = true
	return stmt, nil
}

func (conn *Connection) newStmt() (*Statement, error) {
	stmt := &Statement{}

	if ret := C.SQLAllocHandle(C.SQL_HANDLE_STMT, conn.Dbc, &stmt.handle); !Success(ret) {
		return nil, FormatError(C.SQL_HANDLE_DBC, conn.Dbc)
	}
	return stmt, nil
}

func (conn *Connection) Prepare(sql string, params ...interface{}) (*Statement, error) {
	wsql := StringToUTF16Ptr(sql)

	stmt, err := conn.newStmt()
	if err != nil {
		return nil, err
	}

	if ret := C.SQLPrepareW(
		C.SQLHSTMT(stmt.handle),
		(*C.SQLWCHAR)(unsafe.Pointer(wsql)),
		C.SQLINTEGER(len(sql))); !Success(ret) {
		err := FormatError(C.SQL_HANDLE_STMT, stmt.handle)
		stmt.Close()
		return nil, err
	}
	stmt.prepared = true
	return stmt, nil
}

func (conn *Connection) Commit() error {
	if ret := C.SQLEndTran(C.SQL_HANDLE_DBC, conn.Dbc, C.SQL_COMMIT); !Success(ret) {
		return FormatError(C.SQL_HANDLE_DBC, conn.Dbc)
	}
	return nil
}

func (conn *Connection) AutoCommit(b bool) error {
	var n C.int

	if b {
		n = C.SQL_AUTOCOMMIT_ON
	} else {
		n = C.SQL_AUTOCOMMIT_OFF
	}
	if ret := C.SQLSetConnectAttr(
		C.SQLHDBC(conn.Dbc),
		C.SQL_ATTR_AUTOCOMMIT,
		C.SQLPOINTER(unsafe.Pointer(uintptr(n))),
		C.SQL_IS_UINTEGER); !Success(ret) {
		return FormatError(C.SQL_HANDLE_DBC, conn.Dbc)
	}
	return nil
}

func (conn *Connection) BeginTransaction() error {
	if ret := C.SQLSetConnectAttr(
		C.SQLHDBC(conn.Dbc),
		C.SQL_ATTR_AUTOCOMMIT,
		C.SQLPOINTER(unsafe.Pointer(uintptr(C.SQL_AUTOCOMMIT_OFF))),
		C.SQL_IS_UINTEGER); !Success(ret) {
		return FormatError(C.SQL_HANDLE_DBC, conn.Dbc)
	}
	return nil
}

func (conn *Connection) Rollback() error {
	if ret := C.SQLEndTran(C.SQL_HANDLE_DBC, conn.Dbc, C.SQL_ROLLBACK); !Success(ret) {
		return FormatError(C.SQL_HANDLE_DBC, conn.Dbc)
	}
	return nil
}

// ServerInfo fetch info regarding the underlying database server
func (conn *Connection) ServerInfo() (dbName, dbVersion, serverName string, err error) {
	var (
		infoLen C.SQLSMALLINT
		p       = make([]byte, infoBufferLen)
	)

	if ret := C.SQLGetInfo(
		C.SQLHDBC(conn.Dbc),
		C.SQL_DATABASE_NAME,
		C.SQLPOINTER(unsafe.Pointer(&p[0])),
		infoBufferLen,
		&infoLen); !Success(ret) {
		return "", "", "", FormatError(C.SQL_HANDLE_DBC, conn.Dbc)
	}
	dbName = string(p[0:infoLen])

	if ret := C.SQLGetInfo(
		C.SQLHDBC(conn.Dbc),
		C.SQL_DBMS_VER,
		C.SQLPOINTER(unsafe.Pointer(&p[0])),
		infoBufferLen,
		&infoLen); !Success(ret) {
		return dbName, "", "", FormatError(C.SQL_HANDLE_DBC, conn.Dbc)
	}
	dbVersion = string(p[0:infoLen])

	if ret := C.SQLGetInfo(
		C.SQLHDBC(conn.Dbc),
		C.SQL_SERVER_NAME,
		C.SQLPOINTER(unsafe.Pointer(&p[0])),
		infoBufferLen,
		&infoLen); !Success(ret) {
		return dbName, dbVersion, "", FormatError(C.SQL_HANDLE_DBC, conn.Dbc)
	}
	serverName = string(p[0:infoLen])

	return dbName, dbVersion, serverName, nil
}

// ClientInfo fetch info regarding the client's driver.
func (conn *Connection) ClientInfo() (driverName string, odbcVersion string, driverVersion string, err error) {
	var (
		infoLen C.SQLSMALLINT
		p       = make([]byte, infoBufferLen)
	)

	if ret := C.SQLGetInfo(
		C.SQLHDBC(conn.Dbc),
		C.SQL_DRIVER_NAME,
		C.SQLPOINTER(unsafe.Pointer(&p[0])),
		infoBufferLen,
		&infoLen); !Success(ret) {
		return "", "", "", FormatError(C.SQL_HANDLE_DBC, conn.Dbc)
	}
	driverName = string(p[0:infoLen])

	if ret := C.SQLGetInfo(
		C.SQLHDBC(conn.Dbc),
		C.SQL_DRIVER_ODBC_VER,
		C.SQLPOINTER(unsafe.Pointer(&p[0])),
		infoBufferLen,
		&infoLen); !Success(ret) {
		return "", "", "", FormatError(C.SQL_HANDLE_DBC, conn.Dbc)
	}
	odbcVersion = string(p[0:infoLen])

	if ret := C.SQLGetInfo(
		C.SQLHDBC(conn.Dbc),
		C.SQL_DRIVER_VER,
		C.SQLPOINTER(unsafe.Pointer(&p[0])),
		infoBufferLen,
		&infoLen); !Success(ret) {
		err := FormatError(C.SQL_HANDLE_DBC, conn.Dbc)
		return "", "", "", err
	}
	driverVersion = string(p[0:infoLen])

	return driverName, odbcVersion, driverVersion, nil
}

func (conn *Connection) Close() error {
	if conn.connected {
		if ret := C.SQLDisconnect(C.SQLHDBC(conn.Dbc)); !Success(ret) {
			return FormatError(C.SQL_HANDLE_DBC, conn.Dbc)
		}

		if ret := C.SQLFreeHandle(C.SQL_HANDLE_DBC, conn.Dbc); !Success(ret) {
			return FormatError(C.SQL_HANDLE_DBC, conn.Dbc)
		}
		conn.connected = false
	}
	return nil
}

func (stmt *Statement) RowsAffected() (int, error) {
	var nor C.SQLLEN

	if ret := C.SQLRowCount(C.SQLHSTMT(stmt.handle), &nor); !Success(ret) {
		return -1, FormatError(C.SQL_HANDLE_STMT, stmt.handle)
	}
	return int(nor), nil
}

func (stmt *Statement) Cancel() error {
	ret := C.SQLCancel(C.SQLHSTMT(stmt.handle))
	if !Success(ret) {
		err := FormatError(C.SQL_HANDLE_STMT, stmt.handle)
		return err
	}
	return nil
}

func (stmt *Statement) NumParams() int {
	var cParams C.SQLSMALLINT

	if ret := C.SQLNumParams(C.SQLHSTMT(stmt.handle), &cParams); !Success(ret) {
		return -1
	}
	return int(cParams)
}

func (stmt *Statement) Execute(params ...interface{}) error {
	if params != nil {
		var cParams C.SQLSMALLINT

		if ret := C.SQLNumParams(C.SQLHSTMT(stmt.handle), &cParams); !Success(ret) {
			err := FormatError(C.SQL_HANDLE_STMT, stmt.handle)
			return err
		}
		for i := 0; i < int(cParams); i++ {
			stmt.BindParam(i+1, params[i])
		}
	}
	ret := C.SQLExecute(C.SQLHSTMT(stmt.handle))
	if ret == C.SQL_NEED_DATA {
		// TODO
		//		send_data(stmt)
	} else if ret == C.SQL_NO_DATA {
		// Execute NO DATA
	} else if !Success(ret) {
		return FormatError(C.SQL_HANDLE_STMT, stmt.handle)
	}
	stmt.executed = true
	return nil
}

func (stmt *Statement) Execute2(params []driver.Value) error {
	if params != nil {
		var cParams C.SQLSMALLINT

		if ret := C.SQLNumParams(C.SQLHSTMT(stmt.handle), &cParams); !Success(ret) {
			err := FormatError(C.SQL_HANDLE_STMT, stmt.handle)
			return err
		}
		for i := 0; i < int(cParams); i++ {
			stmt.BindParam(i+1, params[i])
		}
	}
	if ret := C.SQLExecute(C.SQLHSTMT(stmt.handle)); ret == C.SQL_NEED_DATA {
		// TODO
		//		send_data(stmt)
	} else if ret == C.SQL_NO_DATA {
		// Execute NO DATA
	} else if !Success(ret) {
		err := FormatError(C.SQL_HANDLE_STMT, stmt.handle)
		return err
	}
	stmt.executed = true
	return nil
}

func (stmt *Statement) Fetch() (bool, error) {
	ret := C.SQLFetch(C.SQLHSTMT(stmt.handle))
	if ret == C.SQL_NO_DATA {
		return false, nil
	}
	if !Success(ret) {
		return false, FormatError(C.SQL_HANDLE_STMT, stmt.handle)
	}
	return true, nil
}

type Row struct {
	Data []interface{}
}

// Get (Columnindex)
// TODO: Get(ColumnName)
func (r *Row) Get(a interface{}) interface{} {
	value := reflect.ValueOf(a)
	switch f := value; f.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return r.Data[f.Int()]
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return r.Data[f.Uint()]
		//	case *reflect.StringValue:
		//		i := r.Meta[f.Get()]
		//		return r.Data[i]
	}
	return nil
}

func (r *Row) GetInt(a interface{}) (ret int64) {
	v := r.Get(a)
	value := reflect.ValueOf(v)
	switch f := value; f.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		ret = int64(f.Int())
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		ret = int64(f.Uint())
	}
	return
}

func (r *Row) GetFloat(a interface{}) float64 {
	var (
		ret   float64
		v     = r.Get(a)
		value = reflect.ValueOf(v)
	)

	switch f := value; f.Kind() {
	case reflect.Float32, reflect.Float64:
		ret = float64(f.Float())
	}
	return ret
}

func (r *Row) GetString(a interface{}) string {
	var (
		ret   string
		v     = r.Get(a)
		value = reflect.ValueOf(v)
	)

	switch f := value; f.Kind() {
	case reflect.String:
		ret = f.String()
	}
	return ret
}

func (r *Row) Length() int {
	return len(r.Data)
}

func (stmt *Statement) FetchAll() (rows []*Row, err error) {
	for {
		row, err := stmt.FetchOne()
		if err != nil || row == nil {
			break
		}
		rows = append(rows, row)
	}

	return rows, err
}

func (stmt *Statement) FetchOne() (*Row, error) {
	ok, err := stmt.Fetch()
	if !ok {
		return nil, err
	}
	n, _ := stmt.NumFields()
	row := new(Row)
	row.Data = make([]interface{}, n)
	for i := 0; i < n; i++ {
		v, _, _, _ := stmt.GetField(i)
		row.Data[i] = v
	}
	return row, nil
}

func (stmt *Statement) FetchOne2(row []driver.Value) (eof bool, err error) {
	ok, err := stmt.Fetch()
	if !ok && err == nil {
		return !ok, nil
	} else if err != nil {
		return false, err
	}
	n, _ := stmt.NumFields()
	for i := 0; i < n; i++ {
		v, _, _, _ := stmt.GetField(i)
		row[i] = v
	}
	return false, nil
}

// GetField .
func (stmt *Statement) GetField(fieldIndex int) (v interface{}, ftype int, flen int, err error) {
	var (
		fieldType C.int
		fieldLen  C.SQLLEN
		ll        C.SQLSMALLINT
	)

	if ret := C._SQLColAttribute(
		C.SQLHSTMT(stmt.handle),
		C.SQLUSMALLINT(fieldIndex+1),
		C.SQL_DESC_CONCISE_TYPE,
		nil,
		C.SQLSMALLINT(0),
		&ll,
		unsafe.Pointer(&fieldType)); !Success(ret) {
		return nil, -1, -1, FormatError(C.SQL_HANDLE_STMT, stmt.handle)
	}
	if ret := C._SQLColAttribute(
		C.SQLHSTMT(stmt.handle),
		C.SQLUSMALLINT(fieldIndex+1),
		C.SQL_DESC_LENGTH,
		nil,
		C.SQLSMALLINT(0),
		&ll,
		unsafe.Pointer(&fieldLen)); !Success(ret) {
		return nil, -1, -1, FormatError(C.SQL_HANDLE_STMT, stmt.handle)
	}

	var (
		ret C.SQLRETURN
		fl  = C.SQLLEN(fieldLen)
	)

	switch int(fieldType) {
	case C.SQL_BIT:
		var value C.BYTE
		ret = C.SQLGetData(C.SQLHSTMT(stmt.handle), C.SQLUSMALLINT(fieldIndex+1), C.SQL_C_BIT, C.SQLPOINTER(unsafe.Pointer(&value)), 0, &fl)
		if fl == -1 {
			v = nil
		} else {
			v = byte(value)
		}
	case C.SQL_INTEGER, C.SQL_SMALLINT, C.SQL_TINYINT:
		var value C.long
		ret = C.SQLGetData(C.SQLHSTMT(stmt.handle), C.SQLUSMALLINT(fieldIndex+1), C.SQL_C_LONG, C.SQLPOINTER(unsafe.Pointer(&value)), 0, &fl)
		if fl == -1 {
			v = nil
		} else {
			v = int(value)
		}
	case C.SQL_BIGINT:
		var value C.longlong
		ret = C.SQLGetData(C.SQLHSTMT(stmt.handle), C.SQLUSMALLINT(fieldIndex+1), C.SQL_C_SBIGINT, C.SQLPOINTER(unsafe.Pointer(&value)), 0, &fl)
		if fl == -1 {
			v = nil
		} else {
			v = int64(value)
		}
	case C.SQL_FLOAT, C.SQL_REAL, C.SQL_DOUBLE:
		var value C.double
		ret = C.SQLGetData(C.SQLHSTMT(stmt.handle), C.SQLUSMALLINT(fieldIndex+1), C.SQL_C_DOUBLE, C.SQLPOINTER(unsafe.Pointer(&value)), 0, &fl)
		if fl == -1 {
			v = nil
		} else {
			v = float64(value)
		}
	case C.SQL_NUMERIC:
		var value = make([]byte, fl)
		ret = C.SQLGetData(
			C.SQLHSTMT(stmt.handle),
			C.SQLUSMALLINT(fieldIndex+1),
			C.SQL_C_NUMERIC,
			C.SQLPOINTER(unsafe.Pointer(&value[0])),
			fl,
			&fl)
		if fl == -1 {
			v = nil
		} else {
			v = value[:fl]
		}
	case C.SQL_CHAR, C.SQL_VARCHAR, C.SQL_LONGVARCHAR, C.SQL_WCHAR, C.SQL_WVARCHAR, C.SQL_WLONGVARCHAR:
		value := make([]uint16, int(fieldLen)+8)
		ret = C.SQLGetData(
			C.SQLHSTMT(stmt.handle),
			C.SQLUSMALLINT(fieldIndex+1),
			C.SQL_C_WCHAR,
			C.SQLPOINTER(unsafe.Pointer(&value[0])),
			fieldLen+4,
			&fl)
		s := UTF16ToString(value)
		v = s
	case C.SQL_TYPE_TIMESTAMP, C.SQL_TYPE_DATE, C.SQL_TYPE_TIME, C.SQL_DATETIME:
		var value C.TIMESTAMP_STRUCT
		ret = C.SQLGetData(
			C.SQLHSTMT(stmt.handle),
			C.SQLUSMALLINT(fieldIndex+1),
			C.SQL_C_TYPE_TIMESTAMP,
			C.SQLPOINTER(unsafe.Pointer(&value)),
			C.SQLLEN(unsafe.Sizeof(value)),
			&fl)
		if fl == -1 {
			v = nil
		} else {
			v = time.Date(int(value.year), time.Month(value.month), int(value.day), int(value.hour), int(value.minute), int(value.second), int(value.fraction), time.UTC)
		}
	case C.SQL_BINARY, C.SQL_VARBINARY, C.SQL_LONGVARBINARY:
		var vv int
		ret = C.SQLGetData(
			C.SQLHSTMT(stmt.handle),
			C.SQLUSMALLINT(fieldIndex+1),
			C.SQL_C_BINARY,
			C.SQLPOINTER(unsafe.Pointer(&vv)),
			0,
			&fl)
		if fl == -1 {
			v = nil
		} else {
			value := make([]byte, fl)
			ret = C.SQLGetData(
				C.SQLHSTMT(stmt.handle),
				C.SQLUSMALLINT(fieldIndex+1),
				C.SQL_C_BINARY,
				C.SQLPOINTER(unsafe.Pointer(&value[0])),
				C.SQLLEN(fl),
				&fl)
			v = value
		}
	default:
		value := make([]byte, fieldLen)
		ret = C.SQLGetData(
			C.SQLHSTMT(stmt.handle),
			C.SQLUSMALLINT(fieldIndex+1),
			C.SQL_C_BINARY,
			C.SQLPOINTER(unsafe.Pointer(&value[0])),
			fieldLen,
			&fl)
		v = value
	}
	if !Success(ret) {
		return v, int(fieldType), int(fl), FormatError(C.SQL_HANDLE_STMT, stmt.handle)
	}
	return v, int(fieldType), int(fl), err
}

func (stmt *Statement) NumFields() (int, error) {
	var NOC C.SQLSMALLINT

	if ret := C.SQLNumResultCols(C.SQLHSTMT(stmt.handle), &NOC); !Success(ret) {
		return -1, FormatError(C.SQL_HANDLE_STMT, stmt.handle)
	}
	return int(NOC), nil
}

func (stmt *Statement) GetParamType(index int) (int, int, int, int, error) {
	var (
		dataType C.SQLSMALLINT
		decPtr   C.SQLSMALLINT
		nullPtr  C.SQLSMALLINT
		sizePtr  C.SQLULEN
	)

	if ret := C.SQLDescribeParam(
		C.SQLHSTMT(stmt.handle),
		C.SQLUSMALLINT(index),
		&dataType,
		&sizePtr,
		&decPtr,
		&nullPtr); !Success(ret) {
		return -1, -1, -1, -1, FormatError(C.SQL_HANDLE_STMT, stmt.handle)
	}
	return int(dataType), int(sizePtr), int(decPtr), int(nullPtr), nil
}

func (stmt *Statement) BindParam(index int, param interface{}) error {
	var (
		ValueType         C.SQLSMALLINT
		ParameterType     C.SQLSMALLINT
		ColumnSize        C.SQLULEN
		DecimalDigits     C.SQLSMALLINT
		ParameterValuePtr C.SQLPOINTER
		BufferLength      C.SQLLEN
		StrlenOrIndPt     C.SQLLEN
	)

	v := reflect.ValueOf(param)
	if param == nil {
		ft, _, _, _, err := stmt.GetParamType(index)
		if err != nil {
			return err
		}
		ParameterType = C.SQLSMALLINT(ft)
		if ParameterType == C.SQL_UNKNOWN_TYPE {
			ParameterType = C.SQL_VARCHAR
		}
		ValueType = C.SQL_C_DEFAULT
		StrlenOrIndPt = C.SQL_NULL_DATA
		ColumnSize = 1
	} else {
		switch v.Kind() {
		case reflect.Bool:
			ParameterType = C.SQL_BIT
			ValueType = C.SQL_C_BIT
			var b [1]byte
			if v.Bool() {
				b[0] = 1
			} else {
				b[0] = 0
			}
			ParameterValuePtr = C.SQLPOINTER(unsafe.Pointer(&b[0]))
			BufferLength = 1
			StrlenOrIndPt = 0
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			switch v.Type().Kind() {
			case reflect.Int:
				fallthrough
			case reflect.Int8, reflect.Int16, reflect.Int32:
				ParameterType = C.SQL_INTEGER
				ValueType = C.SQL_C_LONG
				var l = C.long(v.Int())
				ParameterValuePtr = C.SQLPOINTER(unsafe.Pointer(&l))
				BufferLength = 4
				StrlenOrIndPt = 0
			case reflect.Int64:
				ParameterType = C.SQL_BIGINT
				ValueType = C.SQL_C_SBIGINT
				var ll = C.longlong(v.Int())
				ParameterValuePtr = C.SQLPOINTER(unsafe.Pointer(&ll))
				BufferLength = 8
				StrlenOrIndPt = 0
			}
		case reflect.Float32, reflect.Float64:
			ParameterType = C.SQL_DOUBLE
			ValueType = C.SQL_C_DOUBLE
			var d = C.double(v.Float())
			ParameterValuePtr = C.SQLPOINTER(unsafe.Pointer(&d))
			BufferLength = 8
			StrlenOrIndPt = 0
		case reflect.String:
			var slen = C.SQLUINTEGER(len(v.String()))
			ParameterType = C.SQL_VARCHAR
			ValueType = C.SQL_C_CHAR
			s := []byte(v.String())
			ParameterValuePtr = C.SQLPOINTER(unsafe.Pointer(&s[0]))
			ColumnSize = C.SQLULEN(slen)
			BufferLength = C.SQLLEN(slen + 1)
			StrlenOrIndPt = C.SQLLEN(slen)
		default:
			fmt.Println("Not support type", v)
		}
	}

	if ret := C.SQLBindParameter(
		C.SQLHSTMT(stmt.handle),
		C.SQLUSMALLINT(index),
		C.SQL_PARAM_INPUT,
		ValueType,
		ParameterType,
		ColumnSize,
		DecimalDigits,
		ParameterValuePtr,
		BufferLength,
		&StrlenOrIndPt); !Success(ret) {
		return FormatError(C.SQL_HANDLE_STMT, stmt.handle)
	}
	return nil
}

func (stmt *Statement) NextResult() bool {
	return C.SQLMoreResults(C.SQLHSTMT(stmt.handle)) != C.SQL_NO_DATA
}

func (stmt *Statement) NumRows() (int, error) {
	var NOR C.SQLLEN

	if ret := C.SQLRowCount(C.SQLHSTMT(stmt.handle), &NOR); !Success(ret) {
		return -1, FormatError(C.SQL_HANDLE_STMT, stmt.handle)
	}
	return int(NOR), nil
}

func (stmt *Statement) HasRows() bool {
	n, _ := stmt.NumRows()
	return n > 0
}

type Field struct {
	Name          string
	Type          int
	Size          int
	DecimalDigits int
	Nullable      int
}

func (stmt *Statement) FieldMetadata(col int) (*Field, error) {
	var (
		BufferLength  C.SQLSMALLINT = infoBufferLen
		NameLength    C.SQLSMALLINT
		DataType      C.SQLSMALLINT
		ColumnSize    C.SQLULEN
		DecimalDigits C.SQLSMALLINT
		Nullable      C.SQLSMALLINT
	)
	ColumnName := make([]byte, infoBufferLen)
	if ret := C.SQLDescribeCol(C.SQLHSTMT(stmt.handle),
		C.SQLUSMALLINT(col),
		(*C.SQLCHAR)(unsafe.Pointer(&ColumnName[0])),
		BufferLength,
		&NameLength,
		&DataType,
		&ColumnSize,
		&DecimalDigits,
		&Nullable); !Success(ret) {
		return nil, FormatError(C.SQL_HANDLE_STMT, stmt.handle)
	}
	return &Field{
		Name:          string(ColumnName[0:NameLength]),
		Type:          int(DataType),
		Size:          int(ColumnSize),
		DecimalDigits: int(DecimalDigits),
		Nullable:      int(Nullable),
	}, nil
}

func (stmt *Statement) free() {
	C.SQLFreeHandle(C.SQL_HANDLE_STMT, stmt.handle)
}

func (stmt *Statement) Close() {
	stmt.free()
}

func Success(ret C.SQLRETURN) bool {
	return int(ret) == C.SQL_SUCCESS || int(ret) == C.SQL_SUCCESS_WITH_INFO
}

func FormatError(ht C.SQLSMALLINT, h C.SQLHANDLE) error {
	var (
		nativeError C.SQLINTEGER
		textLength  C.SQLSMALLINT
	)
	var (
		sqlState    = make([]uint16, 6)
		messageText = make([]uint16, C.SQL_MAX_MESSAGE_LENGTH)
		err         = &Error{}
	)

	for i := 0; ; i++ {
		if ret := C.SQLGetDiagRecW(
			C.SQLSMALLINT(ht),
			h,
			C.SQLSMALLINT(i+1),
			(*C.SQLWCHAR)(unsafe.Pointer(&sqlState[0])),
			&nativeError,
			(*C.SQLWCHAR)(unsafe.Pointer(&messageText[0])),
			C.SQL_MAX_MESSAGE_LENGTH,
			&textLength); ret == C.SQL_INVALID_HANDLE || ret == C.SQL_NO_DATA {
			break
		}
		if i == 0 { // first error message save the SQLSTATE.
			err.SQLState = UTF16ToString(sqlState)
			err.NativeError = int(nativeError)
		}
		err.ErrorMessage += UTF16ToString(messageText)
	}

	return err
}
