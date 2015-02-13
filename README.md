ODBC database driver for Go

Install:
```go
go get github.com/weigj/go-odbc
```

Example:

```go
package main

import (
	"log"

	odbc "github.com/weigj/go-odbc"
)

func main() {
	conn, err := odbc.Connect("DSN=dsn;UID=user;PWD=password")
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	stmt, err := conn.Prepare("select * from user where username = ?")
	if err != nil {
		log.Fatal(err)
	}
	defer stmt.Close()

	if err := stmt.Execute("admin"); err != nil {
		log.Fatal(err)
	}

	rows, err := stmt.FetchAll()
	if err != nil {
		log.Fatal(err)
	}
	for i, row := range rows {
		println(i, row)
	}
}
```

Tested on:
- SQL Server 2005 and Windows 7
- SQL Server 2005 and Ubuntu 10.4 (UnixODBC+FreeTDS)
- Oracle 10g and Windows 7
- Vertica 6.2 (UnixODBC)
