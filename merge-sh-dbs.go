package main

import (
	"database/sql"
	"fmt"
	"os"
	"runtime/debug"
	"strconv"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

const nilStr string = "<nil>"
const emailStr string = ", email:"

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

// profile holds data for profiles table
type profile struct {
	uuid        string
	name        *string
	email       *string
	gender      *string
	genderAcc   *int64
	isBot       *int
	countryCode *string
}

func (p profile) String() string {
	s := "{uuid:" + p.uuid + ", name:"
	if p.name != nil {
		s += *p.name
	} else {
		s += nilStr
	}
	s += emailStr
	if p.email != nil {
		s += *p.email
	} else {
		s += nilStr
	}
	s += ", gender:"
	if p.gender != nil {
		s += *p.gender
	} else {
		s += nilStr
	}
	s += ", genderAcc:"
	if p.genderAcc != nil {
		s += strconv.Itoa(int(*p.genderAcc))
	} else {
		s += nilStr
	}
	s += ", isBot:"
	if p.isBot != nil {
		s += strconv.Itoa(*p.isBot)
	} else {
		s += nilStr
	}
	s += ", countryCode:"
	if p.countryCode != nil {
		s += *p.countryCode
	} else {
		s += nilStr
	}
	s += "}"
	return s
}

// profilesDiffer: cmpare two profile with the same uuid
func profilesDiffer(p1, p2 *profile) bool {
	if p1.name == nil && p2.name != nil || p1.name != nil && p2.name == nil {
		return true
	}
	if p1.name != nil && p2.name != nil && *p1.name != *p2.name {
		return true
	}
	if p1.email == nil && p2.email != nil || p1.email != nil && p2.email == nil {
		return true
	}
	if p1.email != nil && p2.email != nil && *p1.email != *p2.email {
		return true
	}
	if p1.genderAcc == nil && p2.genderAcc != nil || p1.genderAcc != nil && p2.genderAcc == nil {
		return true
	}
	if p1.genderAcc != nil && p2.genderAcc != nil && *p1.genderAcc != *p2.genderAcc {
		return true
	}
	if p1.isBot == nil && p2.isBot != nil || p1.isBot != nil && p2.isBot == nil {
		return true
	}
	if p1.isBot != nil && p2.isBot != nil && *p1.isBot != *p2.isBot {
		return true
	}
	if p1.countryCode == nil && p2.countryCode != nil || p1.countryCode != nil && p2.countryCode == nil {
		return true
	}
	if p1.countryCode != nil && p2.countryCode != nil && *p1.countryCode != *p2.countryCode {
		return true
	}
	return false
}

func mergeProfiles(p1, p2 *profile) profile {
	var p profile
	p.uuid = p1.uuid
	p.name = p1.name
	if p1.name == nil && p2.name != nil {
		p.name = p2.name
	}
	p.email = p1.email
	if p1.email == nil && p2.email != nil {
		p.email = p2.email
	}
	p.gender = p1.gender
	if p1.gender == nil && p2.gender != nil {
		p.gender = p2.gender
	}
	p.genderAcc = p1.genderAcc
	if p1.genderAcc == nil && p2.genderAcc != nil {
		p.genderAcc = p2.genderAcc
	}
	if p1.genderAcc != nil && p2.genderAcc != nil {
		if *p1.genderAcc > *p2.genderAcc && p1.gender != nil {
			p.genderAcc = p1.genderAcc
			p.gender = p1.gender
		}
		if *p2.genderAcc > *p1.genderAcc && p2.gender != nil {
			p.genderAcc = p2.genderAcc
			p.gender = p2.gender
		}
	}
	p.isBot = p1.isBot
	if p1.isBot == nil && p2.isBot != nil {
		p.isBot = p2.isBot
	}
	p.countryCode = p1.countryCode
	if p1.countryCode == nil && p2.countryCode != nil {
		p.countryCode = p2.countryCode
	}
	return p
}

// identity holds data for indentities table
type identity struct {
	id           string
	name         *string
	email        *string
	username     *string
	source       string
	uuid         *string
	lastModified *time.Time
}

func (i identity) String() string {
	s := "{id:" + i.id + ", name:"
	if i.name != nil {
		s += *i.name
	} else {
		s += nilStr
	}
	s += emailStr
	if i.email != nil {
		s += *i.email
	} else {
		s += nilStr
	}
	s += ", username:"
	if i.username != nil {
		s += *i.username
	} else {
		s += nilStr
	}
	s += ", source:" + i.source
	s += ", uuid:"
	if i.uuid != nil {
		s += *i.uuid
	} else {
		s += nilStr
	}
	s += fmt.Sprintf(", lastModified:%+v}", i.lastModified)
	return s
}

func identitiesDiffer(i1, i2 *identity) bool {
	if i1.source != i2.source {
		return true
	}
	if i1.name == nil && i2.name != nil || i1.name != nil && i2.name == nil {
		return true
	}
	if i1.name != nil && i2.name != nil && *i1.name != *i2.name {
		return true
	}
	if i1.email == nil && i2.email != nil || i1.email != nil && i2.email == nil {
		return true
	}
	if i1.email != nil && i2.email != nil && *i1.email != *i2.email {
		return true
	}
	if i1.uuid == nil && i2.uuid != nil || i1.uuid != nil && i2.uuid == nil {
		return true
	}
	if i1.uuid != nil && i2.uuid != nil && *i1.uuid != *i2.uuid {
		return true
	}
	return false
}

func mergeIdentities(i1, i2 *identity) identity {
	var i identity
	i.id = i1.id
	i.name = i1.name
	if i1.name == nil && i2.name != nil {
		i.name = i2.name
	}
	i.email = i1.email
	if i1.email == nil && i2.email != nil {
		i.email = i2.email
	}
	i.username = i1.username
	if i1.username == nil && i2.username != nil {
		i.username = i2.username
	}
	i.uuid = i1.uuid
	if i1.uuid == nil && i2.uuid != nil {
		i.uuid = i2.uuid
	}
	i.lastModified = i1.lastModified
	if i1.lastModified == nil && i2.lastModified != nil {
		i.lastModified = i2.lastModified
	}
	i.source = i1.source
	if i1.lastModified != nil && i2.lastModified != nil {
		if (*i1.lastModified).After(*i.lastModified) {
			i.source = i1.source
		} else {
			i.lastModified = i2.lastModified
			i.source = i2.source
		}
	}
	return i
}

// enrollment holds data for enrollments table
type enrollment struct {
	id          int64
	start       time.Time
	end         time.Time
	uuid        string
	orgID       int64
	orgName     string
	orgIDMerged int64
}

// We are comparing enrollments using 'enrollmentkey' which already contains uuid, start, to
// we are skipiing id filed because it is an auto incrementing PK, so we only have to compare
// organization, we're not comparing its ID because it can be different on different databases (auto incrementing key)
// So basically we only need to compare orgNames
func enrollmentsDiffer(e1, e2 *enrollment) bool {
	return e1.orgName != e2.orgName
}

// enrollmentKey holds key data for the enrollment
type enrollmentKey struct {
	start time.Time
	end   time.Time
	uuid  string
}

// mergeDatabases merged dbs[0] and dbs[1] into dbs[2]
func mergeDatabases(dbs []*sql.DB) error {
	dbg := os.Getenv("DEBUG") != ""
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
			if dbg {
				fmt.Printf("Country from 1st (%+v) missing in 2nd, adding\n", c)
			}
			continue
		}
		if c.name != c2.name || c.alpha3 != c2.alpha3 {
			fmt.Printf("Country from 1st (%+v) different in 2nd (%+v), using first\n", c, c2)
		}
	}
	for code, c := range countryMap[1] {
		c1, ok := countryMap[0][code]
		if !ok {
			if dbg {
				fmt.Printf("Country from 2nd (%+v) missing in 1st, adding\n", c)
			}
			countryMap[2][code] = c
			continue
		}
		if c.name != c1.name || c.alpha3 != c1.alpha3 {
			fmt.Printf("Country from 2nd (%+v) different in 1st (%+v), using first\n", c, c1)
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
		if dbg && !ok {
			fmt.Printf("Organization from 1st (id=%d, name=%s) missing in 2nd, adding\n", id, name)
		}
	}
	for name, id := range orgStr2ID[1] {
		_, ok := orgStr2ID[0][name]
		if dbg && !ok {
			fmt.Printf("Organization from 2nd (id=%d, name=%s) missing in 1st, adding\n", id, name)
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
	var domID2Str [3]map[int64]string
	var domStr2ID [3]map[string]int64
	domStr := make(map[string]string)
	for i := 0; i < 2; i++ {
		rows, err := dbs[i].Query("select id, domain, is_top_domain, organization_id from domains_organizations")
		fatalOnError(err)
		var do domainOrg
		domainMap[i] = make(map[int64]domainOrg)
		domID2Str[i] = make(map[int64]string)
		domStr2ID[i] = make(map[string]int64)
		for rows.Next() {
			fatalOnError(rows.Scan(&do.id, &do.domain, &do.isTopDomain, &do.orgID))
			// Map into merged organization_id - must succeed
			orgName, ok := orgID2Str[i][do.orgID]
			if !ok {
				fatalf("cannot map organization ID %d from #%d input database", do.orgID, i+1)
			}
			do.orgName = orgName
			orgIDMerged, ok := orgStr2ID[2][strings.ToLower(do.orgName)]
			if !ok {
				fatalf("cannot map organization ID %d -> Name %s from #%d input database", do.orgID, do.orgName, i+1)
			}
			do.orgIDMerged = orgIDMerged
			domainMap[i][do.id] = do
			domID2Str[i][do.id] = do.domain
			domStr2ID[i][strings.ToLower(do.domain)] = do.id
			domStr[strings.ToLower(do.domain)] = do.domain
		}
		fatalOnError(rows.Err())
		fatalOnError(rows.Close())
	}
	domainMap[2] = make(map[int64]domainOrg)
	domID2Str[2] = make(map[int64]string)
	domStr2ID[2] = make(map[string]int64)
	domAry := []domainOrg{}
	for domain, id := range domStr2ID[0] {
		_, ok := domStr2ID[1][domain]
		if dbg && !ok {
			fmt.Printf("Domain-Organization from 1st (id=%d, domain=%s, %+v) missing in 2nd, adding\n", id, domain, domainMap[0][id])
		}
		do, ok := domainMap[0][id]
		if !ok {
			fatalf("cannot find domain-organization for id=%d in the first database", id)
		}
		domAry = append(domAry, do)
	}
	for domain, id := range domStr2ID[1] {
		_, ok := domStr2ID[0][domain]
		if !ok {
			if dbg {
				fmt.Printf("Domain-Organization from 2nd (id=%d, domain=%s, %+v) missing in 1st, adding\n", id, domain, domainMap[1][id])
			}
			do, ok := domainMap[1][id]
			if !ok {
				fatalf("cannot find domain-organization for id=%d in the second database", id)
			}
			domAry = append(domAry, do)
		}
	}
	for _, do := range domAry {
		lDomain, ok := domStr[strings.ToLower(do.domain)]
		if !ok {
			fatalf("no mapping for domain %s", do.domain)
		}
		_, err := mdb.Exec("insert into domains_organizations(domain, is_top_domain, organization_id) values(?, ?, ?)", lDomain, do.isTopDomain, do.orgIDMerged)
		fatalOnError(err)
		rows, err := mdb.Query("select id from domains_organizations where domain = ?", lDomain)
		fatalOnError(err)
		var id int64
		for rows.Next() {
			fatalOnError(rows.Scan(&id))
		}
		fatalOnError(rows.Err())
		fatalOnError(rows.Close())
		domID2Str[2][id] = do.domain
		domStr2ID[2][lDomain] = id
	}
	/* matching_blacklist
	+----------+--------------+------+-----+---------+-------+
	| Field    | Type         | Null | Key | Default | Extra |
	+----------+--------------+------+-----+---------+-------+
	| excluded | varchar(128) | NO   | PRI | NULL    |       |
	+----------+--------------+------+-----+---------+-------+
	*/
	fmt.Printf("matching_blacklist...\n")
	_, err = mdb.Exec("delete from matching_blacklist")
	fatalOnError(err)
	blMap := make(map[string]string)
	for i := 0; i < 2; i++ {
		rows, err := dbs[i].Query("select excluded from matching_blacklist")
		fatalOnError(err)
		bl := ""
		for rows.Next() {
			fatalOnError(rows.Scan(&bl))
			blMap[strings.ToLower(bl)] = bl
		}
		fatalOnError(rows.Err())
		fatalOnError(rows.Close())
	}
	for lBl := range blMap {
		_, err := mdb.Exec("insert into matching_blacklist(excluded) values(?)", lBl)
		fatalOnError(err)
	}
	/* uidentities
	+---------------+--------------+------+-----+---------+-------+
	| Field         | Type         | Null | Key | Default | Extra |
	+---------------+--------------+------+-----+---------+-------+
	| uuid          | varchar(128) | NO   | PRI | NULL    |       |
	| last_modified | datetime(6)  | YES  |     | NULL    |       |
	+---------------+--------------+------+-----+---------+-------+
	*/
	fmt.Printf("uidentities...\n")
	_, err = mdb.Exec("delete from uidentities")
	fatalOnError(err)
	var uidMap [3]map[string]time.Time
	for i := 0; i < 2; i++ {
		uidMap[i] = make(map[string]time.Time)
		rows, err := dbs[i].Query("select uuid, last_modified from uidentities")
		fatalOnError(err)
		uuid := ""
		var modified time.Time
		for rows.Next() {
			fatalOnError(rows.Scan(&uuid, &modified))
			uidMap[i][uuid] = modified
		}
		fatalOnError(rows.Err())
		fatalOnError(rows.Close())
	}
	uidMap[2] = make(map[string]time.Time)
	for i := 0; i < 2; i++ {
		for uuid := range uidMap[i] {
			mod1, ok1 := uidMap[0][uuid]
			mod2, ok2 := uidMap[1][uuid]
			if ok1 && !ok2 {
				uidMap[2][uuid] = mod1
			} else if !ok1 && ok2 {
				uidMap[2][uuid] = mod2
			} else if ok1 && ok2 {
				if mod1.After(mod2) {
					uidMap[2][uuid] = mod1
				} else {
					uidMap[2][uuid] = mod2
				}
			} else {
				fatalf("wrong uidentities key %s", uuid)
			}
		}
	}
	for uuid, modified := range uidMap[2] {
		_, err := mdb.Exec("insert into uidentities(uuid, last_modified) values(?, ?)", uuid, modified)
		fatalOnError(err)
	}
	/* profiles
	+--------------+--------------+------+-----+---------+-------+
	| Field        | Type         | Null | Key | Default | Extra |
	+--------------+--------------+------+-----+---------+-------+
	| uuid         | varchar(128) | NO   | PRI | NULL    |       |
	| name         | varchar(128) | YES  |     | NULL    |       |
	| email        | varchar(128) | YES  |     | NULL    |       |
	| gender       | varchar(32)  | YES  |     | NULL    |       |
	| gender_acc   | int(11)      | YES  |     | NULL    |       |
	| is_bot       | tinyint(1)   | YES  |     | NULL    |       |
	| country_code | varchar(2)   | YES  | MUL | NULL    |       |
	+--------------+--------------+------+-----+---------+-------+
	*/
	fmt.Printf("profiles...\n")
	_, err = mdb.Exec("delete from profiles")
	fatalOnError(err)
	var profileMap [3]map[string]profile
	for i := 0; i < 2; i++ {
		rows, err := dbs[i].Query("select uuid, name, email, gender, gender_acc, is_bot, country_code from profiles")
		fatalOnError(err)
		var p profile
		profileMap[i] = make(map[string]profile)
		for rows.Next() {
			fatalOnError(rows.Scan(&p.uuid, &p.name, &p.email, &p.gender, &p.genderAcc, &p.isBot, &p.countryCode))
			profileMap[i][p.uuid] = p
		}
		fatalOnError(rows.Err())
		fatalOnError(rows.Close())
	}
	profileMap[2] = make(map[string]profile)
	for uuid, p := range profileMap[0] {
		p2, ok := profileMap[1][uuid]
		profileMap[2][uuid] = p
		if !ok {
			if dbg {
				fmt.Printf("Profile from 1st (%+v) missing in 2nd, adding\n", p)
			}
			continue
		}
		if profilesDiffer(&p, &p2) {
			fmt.Printf("Profile from 1st (%+v) different in 2nd (%+v), merging\n", p, p2)
			profileMap[2][uuid] = mergeProfiles(&p, &p2)
		}
	}
	for uuid, p := range profileMap[1] {
		p1, ok := profileMap[0][uuid]
		if !ok {
			if dbg {
				fmt.Printf("Profile from 2nd (%+v) missing in 1st, adding\n", p)
			}
			profileMap[2][uuid] = p
			continue
		}
		if profilesDiffer(&p, &p1) {
			fmt.Printf("Profile from 2nd (%+v) different in 1st (%+v), merging\n", p, p1)
			profileMap[2][uuid] = mergeProfiles(&p, &p1)
		}
	}
	for _, p := range profileMap[2] {
		_, err := mdb.Exec("insert into profiles(uuid, name, email, gender, gender_acc, is_bot, country_code) values(?, ?, ?, ?, ?, ?, ?)", p.uuid, p.name, p.email, p.gender, p.genderAcc, p.isBot, p.countryCode)
		fatalOnError(err)
	}
	/* identities
	+---------------+--------------+------+-----+---------+-------+
	| Field         | Type         | Null | Key | Default | Extra |
	+---------------+--------------+------+-----+---------+-------+
	| id            | varchar(128) | NO   | PRI | NULL    |       |
	| name          | varchar(128) | YES  | MUL | NULL    |       |
	| email         | varchar(128) | YES  |     | NULL    |       |
	| username      | varchar(128) | YES  |     | NULL    |       |
	| source        | varchar(32)  | NO   |     | NULL    |       |
	| uuid          | varchar(128) | YES  | MUL | NULL    |       |
	| last_modified | datetime(6)  | YES  |     | NULL    |       |
	+---------------+--------------+------+-----+---------+-------+
	*/
	fmt.Printf("identities...\n")
	_, err = mdb.Exec("delete from identities")
	fatalOnError(err)
	var identityMap [3]map[string]identity
	for i := 0; i < 2; i++ {
		rows, err := dbs[i].Query("select id, name, email, username, source, uuid, last_modified from identities")
		fatalOnError(err)
		var iy identity
		identityMap[i] = make(map[string]identity)
		for rows.Next() {
			fatalOnError(rows.Scan(&iy.id, &iy.name, &iy.email, &iy.username, &iy.source, &iy.uuid, &iy.lastModified))
			identityMap[i][iy.id] = iy
		}
		fatalOnError(rows.Err())
		fatalOnError(rows.Close())
	}
	identityMap[2] = make(map[string]identity)
	for id, i := range identityMap[0] {
		i2, ok := identityMap[1][id]
		identityMap[2][id] = i
		if !ok {
			if dbg {
				fmt.Printf("Identity from 1st (%+v) missing in 2nd, adding\n", i)
			}
			continue
		}
		if identitiesDiffer(&i, &i2) {
			fmt.Printf("Identity from 1st (%+v) different in 2nd (%+v), merging\n", i, i2)
			identityMap[2][id] = mergeIdentities(&i, &i2)
		}
	}
	for id, i := range identityMap[1] {
		i1, ok := identityMap[0][id]
		if !ok {
			if dbg {
				fmt.Printf("Identity from 2nd (%+v) missing in 1st, adding\n", i)
			}
			identityMap[2][id] = i
			continue
		}
		if identitiesDiffer(&i, &i1) {
			fmt.Printf("Identity from 2nd (%+v) different in 1st (%+v), merging\n", i, i1)
			identityMap[2][id] = mergeIdentities(&i, &i1)
		}
	}
	for _, i := range identityMap[2] {
		_, err := mdb.Exec("insert into identities(id, name, email, username, source, uuid, last_modified) values(?, ?, ?, ?, ?, ?, ?)", i.id, i.name, i.email, i.username, i.source, i.uuid, i.lastModified)
		fatalOnError(err)
	}
	/* enrollmants
	+-----------------+--------------+------+-----+---------+----------------+
	| Field           | Type         | Null | Key | Default | Extra          |
	+-----------------+--------------+------+-----+---------+----------------+
	| id              | int(11)      | NO   | PRI | NULL    | auto_increment |
	| start           | datetime     | NO   |     | NULL    |                |
	| end             | datetime     | NO   |     | NULL    |                |
	| uuid            | varchar(128) | NO   | MUL | NULL    |                |
	| organization_id | int(11)      | NO   | MUL | NULL    |                |
	+-----------------+--------------+------+-----+---------+----------------+
	*/
	fmt.Printf("enrollmants...\n")
	_, err = mdb.Exec("delete from enrollmants")
	fatalOnError(err)
	var enrollMap [3]map[enrollmentKey]enrollment
	for i := 0; i < 2; i++ {
		rows, err := dbs[i].Query("select id, start, end, uuid, organization_id from enrollmants")
		fatalOnError(err)
		var (
			e    enrollment
			eKey enrollmentKey
		)
		enrollMap[i] = make(map[enrollmentKey]enrollment)
		for rows.Next() {
			fatalOnError(rows.Scan(&e.id, &e.start, &e.end, &e.uuid, &e.orgID))
			// Map into merged organization_id - must succeed
			orgName, ok := orgID2Str[i][e.orgID]
			if !ok {
				fatalf("cannot map organization ID %d from #%d input database", e.orgID, i+1)
			}
			e.orgName = orgName
			orgIDMerged, ok := orgStr2ID[2][strings.ToLower(e.orgName)]
			if !ok {
				fatalf("cannot map organization ID %d -> Name %s from #%d input database", e.orgID, e.orgName, i+1)
			}
			e.orgIDMerged = orgIDMerged
			eKey.uuid = e.uuid
			eKey.start = e.start
			eKey.end = e.end
			enrollMap[i][eKey] = e
		}
		fatalOnError(rows.Err())
		fatalOnError(rows.Close())
	}
	enrollMap[2] = make(map[enrollmentKey]enrollment)
	for k, e := range enrollMap[0] {
		e2, ok := enrollMap[1][k]
		enrollMap[2][k] = e
		if !ok {
			if dbg {
				fmt.Printf("Enrollment from 1st (%+v) missing in 2nd, adding\n", e)
			}
			continue
		}
		if enrollmentsDiffer(&e, &e2) {
			fmt.Printf("Enrollment from 1st (%+v) different in 2nd (%+v), used first\n", e, e2)
		}
	}
	for k, e := range enrollMap[1] {
		e1, ok := enrollMap[0][k]
		if !ok {
			if dbg {
				fmt.Printf("Enrollment from 2nd (%+v) missing in 1st, adding\n", e)
			}
			enrollMap[2][k] = e
			continue
		}
		if enrollmentsDiffer(&e, &e1) {
			fmt.Printf("Enrollment from 2nd (%+v) different in 1st (%+v), used first\n", e, e1)
		}
	}
	for _, e := range enrollMap[2] {
		_, err := mdb.Exec("insert into enrollments(id, start, end, uuid, organization_id) values(?, ?, ?, ?, ?)", e.id, e.start, e.end, e.uuid, e.orgIDMerged)
		fatalOnError(err)
	}
	return nil
}

// getConnectString - get MariaDB SH (Sorting Hat) database DSN
// Either provide full DSN via SH_DSN='shuser:shpassword@tcp(shhost:shport)/shdb?charset=utf8&parseTime=true'
// Or use some SH_ variables, only SH_PASS is required
// Defaults are: "shuser:required_pwd@tcp(localhost:3306)/shdb?charset=utf8
// SH_DSN has higher priority; if set no SH_ varaibles are used
func getConnectString(prefix string) string {
	//dsn := "shuser:"+os.Getenv("PASS")+"@/shdb?charset=utf8")
	dsn := os.Getenv(prefix + "DSN")
	if dsn == "" {
		pass := os.Getenv(prefix + "PASS")
		user := os.Getenv(prefix + "USER")
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
			params = "?charset=utf8&parseTime=true"
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
