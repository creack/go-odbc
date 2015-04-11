ODBC database driver for Go

Install:
```bash
go get github.com/creack/godbc
```

Example:

```go
package main

import (
	"log"

	"github.com/creack/godbc"
)

func main() {
	conn, err := godbc.Connect("DSN=dsn;UID=user;PWD=password")
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	stmt, err := conn.Prepare("SELECT * FROM user WHERE username = ?")
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
