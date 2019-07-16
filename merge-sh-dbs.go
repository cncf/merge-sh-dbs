package main

import (
	"database/sql"
	"fmt"
	"os"
	"runtime/debug"
	"strings"
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

// domainOrg holds data for domains_organizations table
type domainOrg struct {
	id          int64
	domain      string
	isTopDomain int
	orgID       int64
	orgName     string // computed
	orgIDMerged int64  // computed
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
	fmt.Printf("countries...\n")
	mdb := dbs[2]
	_, err := mdb.Exec("delete from countries")
	fatalOnError(err)
	var countryMap [3]map[string]country
	for i := 0; i < 2; i++ {
		rows, err := dbs[i].Query("select code, name, alpha3 from countries")
		fatalOnError(err)
		var c country
		countryMap[i] = make(map[string]country)
		for rows.Next() {
			fatalOnError(rows.Scan(&c.code, &c.name, &c.alpha3))
			countryMap[i][c.code] = c
		}
		fatalOnError(rows.Err())
		fatalOnError(rows.Close())
	}
	countryMap[2] = make(map[string]country)
	for code, c := range countryMap[0] {
		c2, ok := countryMap[1][code]
		countryMap[2][code] = c
		if !ok {
			fmt.Printf("Country from 1st (%+v) missing in 2nd, adding\n", c)
			continue
		}
		if c.name != c2.name || c.alpha3 != c2.alpha3 {
			fmt.Printf("Country from 1st (%+v) different in 2nd, using first\n", c)
		}
	}
	for code, c := range countryMap[1] {
		c1, ok := countryMap[0][code]
		if !ok {
			fmt.Printf("Country from 2nd (%+v) missing in 1st, adding\n", c)
			countryMap[2][code] = c
			continue
		}
		if c.name != c1.name || c.alpha3 != c1.alpha3 {
			fmt.Printf("Country from 2nd (%+v) different in 1st, using first\n", c)
		}
	}
	for _, c := range countryMap[2] {
		_, err := mdb.Exec("insert into countries(code, name, alpha3) values(?, ?, ?)", c.code, c.name, c.alpha3)
		fatalOnError(err)
	}
	/* organizations
	+-------+--------------+------+-----+---------+----------------+
	| Field | Type         | Null | Key | Default | Extra          |
	+-------+--------------+------+-----+---------+----------------+
	| id    | int(11)      | NO   | PRI | NULL    | auto_increment |
	| name  | varchar(191) | NO   | UNI | NULL    |                |
	+-------+--------------+------+-----+---------+----------------+
	*/
	fmt.Printf("organizations...\n")
	_, err = mdb.Exec("delete from organizations")
	fatalOnError(err)
	var orgID2Str [3]map[int64]string
	var orgStr2ID [3]map[string]int64
	orgStr := make(map[string]string)
	fatalOnError(err)
	for i := 0; i < 2; i++ {
		rows, err := dbs[i].Query("select id, name from organizations")
		fatalOnError(err)
		id := int64(0)
		name := ""
		orgID2Str[i] = make(map[int64]string)
		orgStr2ID[i] = make(map[string]int64)
		for rows.Next() {
			fatalOnError(rows.Scan(&id, &name))
			orgID2Str[i][id] = name
			orgStr2ID[i][strings.ToLower(name)] = id
			orgStr[strings.ToLower(name)] = name
		}
		fatalOnError(rows.Err())
		fatalOnError(rows.Close())
	}
	orgID2Str[2] = make(map[int64]string)
	orgStr2ID[2] = make(map[string]int64)
	for name, id := range orgStr2ID[0] {
		_, ok := orgStr2ID[1][name]
		if !ok {
			fmt.Printf("Organization from 1st (id=%d, name=%s) missing in 2nd, adding\n", id, name)
			continue
		}
	}
	for name, id := range orgStr2ID[1] {
		_, ok := orgStr2ID[0][name]
		if !ok {
			fmt.Printf("Organization from 2nd (id=%d, name=%s) missing in 1st, adding\n", id, name)
			continue
		}
	}
	for lName, name := range orgStr {
		_, err := mdb.Exec("insert into organizations(name) values(?)", name)
		fatalOnError(err)
		rows, err := mdb.Query("select id from organizations where name = ?", name)
		fatalOnError(err)
		var id int64
		for rows.Next() {
			fatalOnError(rows.Scan(&id))
		}
		fatalOnError(rows.Err())
		fatalOnError(rows.Close())
		orgID2Str[2][id] = name
		orgStr2ID[2][lName] = id
	}
	/* domains_organizations
	+-----------------+--------------+------+-----+---------+----------------+
	| Field           | Type         | Null | Key | Default | Extra          |
	+-----------------+--------------+------+-----+---------+----------------+
	| id              | int(11)      | NO   | PRI | NULL    | auto_increment |
	| domain          | varchar(128) | NO   | UNI | NULL    |                |
	| is_top_domain   | tinyint(1)   | YES  |     | NULL    |                |
	| organization_id | int(11)      | NO   | MUL | NULL    |                |
	+-----------------+--------------+------+-----+---------+----------------+
	*/
	fmt.Printf("domains_organizations...\n")
	_, err = mdb.Exec("delete from domains_organizations")
	fatalOnError(err)
	var domainMap [3]map[int64]domainOrg
	for i := 0; i < 2; i++ {
		rows, err := dbs[i].Query("select id, domain, is_top_domain, organization_id from domains_organizations")
		fatalOnError(err)
		var do domainOrg
		domainMap[i] = make(map[int64]domainOrg)
		for rows.Next() {
			fatalOnError(rows.Scan(&do.id, &do.domain, &do.isTopDomain, &do.orgID))
			// Map into merged organization_id - must succeed
			orgName, ok := orgID2Str[i][do.orgID]
			if !ok {
				fatalf("cannot map organization ID %d from #%d input database", do.orgID, i+1)
			}
			do.orgName = orgName
			orgIDMerged, ok := orgStr2ID[i][strings.ToLower(do.orgName)]
			if !ok {
				fatalf("cannot map organization ID %d -> Name %s from #%d input database", do.orgID, do.orgName, i+1)
			}
			do.orgIDMerged = orgIDMerged
			domainMap[i][do.id] = do
		}
		fatalOnError(rows.Err())
		fatalOnError(rows.Close())
	}
	// domainMap[2] = make(map[int64]domainOrg)
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
