package main

import (
	"database/sql"
	"fmt"
	"os"
	"runtime/debug"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

func fatalOnError(err error) {
	if err != nil {
		tm := time.Now()
		fmt.Printf("Error(time=%+v):\nError: '%s'\nStacktrace:\n%s\n", tm, err.Error(), string(debug.Stack()))
		fmt.Fprintf(os.Stderr, "Error(time=%+v):\nError: '%s'\nStacktrace:\n", tm, err.Error())
		panic("stacktrace")
	}
}

func fatalf(f string, a ...interface{}) {
	fatalOnError(fmt.Errorf(f, a...))
}

// country holds data from countries table
type country struct {
	code   string
	name   string
	alpha3 string
}

// mergeDatabases merged dbs[0] and dbs[1] into dbs[2]
func mergeDatabases(dbs []*sql.DB) error {
	/* countries
	+--------+--------------+------+-----+---------+-------+
	| Field  | Type         | Null | Key | Default | Extra |
	+--------+--------------+------+-----+---------+-------+
	| code   | varchar(2)   | NO   | PRI | NULL    |       |
	| name   | varchar(191) | NO   |     | NULL    |       |
	| alpha3 | varchar(3)   | NO   | UNI | NULL    |       |
	+--------+--------------+------+-----+---------+-------+
	*/
	mdb := dbs[2]
	_, err := mdb.Exec("delete from countries")
	fatalOnError(err)
	var cm [3]map[string]country
	for i := 0; i < 2; i++ {
		rows, err := dbs[i].Query("select code, name, alpha3 from countries")
		fatalOnError(err)
		var c country
		cm[i] = make(map[string]country)
		for rows.Next() {
			fatalOnError(rows.Scan(&c.code, &c.name, &c.alpha3))
			cm[i][c.code] = c
		}
		fatalOnError(rows.Err())
		fatalOnError(rows.Close())
	}
	cm[2] = make(map[string]country)
	for code, c := range cm[0] {
		c2, ok := cm[1][code]
		cm[2][code] = c
		if !ok {
			fmt.Printf("Country from 1st (%+v) missing in 2nd, adding\n", c)
			continue
		}
		if c.name != c2.name || c.alpha3 != c2.alpha3 {
			fmt.Printf("Country from 1st (%+v) different in 2nd, using first\n", c)
		}
	}
	for code, c := range cm[1] {
		c1, ok := cm[0][code]
		if !ok {
			fmt.Printf("Country from 2nd (%+v) missing in 1st, adding\n", c)
			cm[2][code] = c
			continue
		}
		if c.name != c1.name || c.alpha3 != c1.alpha3 {
			fmt.Printf("Country from 2nd (%+v) different in 1st, using first\n", c)
		}
	}
	for _, c := range cm[2] {
		_, err := mdb.Exec("insert into countries(code, name, alpha3) values(?, ?, ?)", c.code, c.name, c.alpha3)
		fatalOnError(err)
	}
	return nil
}

// getConnectString - get MariaDB SH (Sorting Hat) database DSN
// Either provide full DSN via SH_DSN='shuser:shpassword@tcp(shhost:shport)/shdb?charset=utf8'
// Or use some SH_ variables, only SH_PASS is required
// Defaults are: "shuser:required_pwd@tcp(localhost:3306)/shdb?charset=utf8
// SH_DSN has higher priority; if set no SH_ varaibles are used
func getConnectString(prefix string) string {
	//dsn := "shuser:"+os.Getenv("PASS")+"@/shdb?charset=utf8")
	dsn := os.Getenv(prefix + "DSN")
	if dsn == "" {
		pass := os.Getenv(prefix + "PASS")
		user := os.Getenv(prefix + "USER")
		if user == "" {
			user = "shuser"
		}
		proto := os.Getenv(prefix + "PROTO")
		if proto == "" {
			proto = "tcp"
		}
		host := os.Getenv(prefix + "HOST")
		if host == "" {
			host = "localhost"
		}
		port := os.Getenv(prefix + "PORT")
		if port == "" {
			port = "3306"
		}
		db := os.Getenv(prefix + "DB")
		if db == "" {
			fatalf("please specify database via %sDB=...", prefix)
		}
		params := os.Getenv(prefix + "PARAMS")
		if params == "" {
			params = "?charset=utf8"
		}
		if params == "-" {
			params = ""
		}
		dsn = fmt.Sprintf(
			"%s:%s@%s(%s:%s)/%s%s",
			user,
			pass,
			proto,
			host,
			port,
			db,
			params,
		)
	}
	return dsn
}

func main() {
	// Connect to MariaDB
	prefixes := []string{"SH1_", "SH2_", "SH_"}
	var dbs []*sql.DB
	for _, prefix := range prefixes {
		dsn := getConnectString(prefix)
		db, err := sql.Open("mysql", dsn)
		dbs = append(dbs, db)
		fatalOnError(err)
		defer func() { fatalOnError(db.Close()) }()
	}
	fatalOnError(mergeDatabases(dbs))
}
