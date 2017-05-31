package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/docopt/docopt-go"
	"github.com/gocraft/dbr"
	_ "github.com/lib/pq"
)

const migrationsTable = "schema_migrations"

var (
	errMissingMigrationFilesTpl = "there are missing migration files: %v"
	errMissingMigrationFileTpl  = "missing migration file with version: %v"
)

func main() {
	usage := `pg-migrate

Usage:
  pg-migrate up <url> [--dir=<dir>] [--steps=<steps>]
  pg-migrate down <url> [--dir=<dir>] [--steps=<steps>]
  pg-migrate create <name>
  pg-migrate -h | --help
  naval_fate --version

Options:
  -h --help        Show help.
  --version        Show version.
  --dir=<dir>      Directory where migrations files are stores. [default: migrations/]
  --steps=<steps>  Max steps to migrate [default: -1].
`
	arguments, _ := docopt.Parse(usage, nil, true, "pg-migrate", false)
	var err error
	if arguments["up"].(bool) {
		log.Println("up")
		url := arguments["<url>"].(string)
		dir := arguments["--dir"].(string)
		var fullDir string
		fullDir, err = filepath.Abs(dir)
		if err != nil {
			log.Fatalln("error:", err)
		}
		var steps int
		steps, err = strconv.Atoi(arguments["--steps"].(string))
		if err != nil {
			log.Fatalln("error:", err)
		}
		err = upCMD(url, fullDir, steps)
	} else if arguments["down"].(bool) {
		log.Println("down")
	} else if arguments["create"].(bool) {
		log.Println("create")
	}
	if err != nil {
		log.Fatalln("error:", err)
	}
}

func upCMD(url, dir string, steps int) error {
	log.Println(url, dir, steps)
	fos, err := getMigrationsFiles(dir, "up")
	if err != nil {
		return err
	}
	_ = fos
	var versions []int
	for _, fo := range fos {
		version, err := getVersion(fo.Name())
		if err != nil {
			return err
		}
		versions = append(versions, version)
	}
	migratedVersions, err := getMigratedVersions(url)
	if err != nil {
		return err
	}
	/*
		ss := superSet(migratedVersions, versions)
		if len(ss) > 0 {
			return fmt.Errorf(errMissingMigrationFilesTpl, ss)
		}
		log.Println(ss)
	*/
	ss2 := superSet(versions, migratedVersions)

	for _, v := range ss2 {
		f, err := getMigrateFile(v, fos)
		if err != nil {
			return err
		}
		if err := doMigrate(url, dir, f); err != nil {
			return err
		}
	}
	log.Println(ss2)
	return nil
}

func doMigrate(url, dir string, file os.FileInfo) error {
	log.Println("migrating:", file.Name())
	dbConn, err := dbr.Open("postgres", url, nil)
	if err != nil {
		return err
	}
	sess := dbConn.NewSession(nil)
	tx, err := sess.Begin()
	if err != nil {
		return err
	}
	defer tx.RollbackUnlessCommitted()
	content, err := ioutil.ReadFile(filepath.Join(dir, file.Name()))
	if err != nil {
		return err
	}
	if _, err := tx.Exec(string(content)); err != nil {
		return err
	}
	version, err := getVersion(file.Name())
	if err != nil {
		return err
	}
	if _, err := tx.InsertInto(migrationsTable).Columns("version").Values(version).Exec(); err != nil {
		return err
	}
	// return tx.Commit()
	return nil
}

func getMigrateFile(version int, fos []os.FileInfo) (os.FileInfo, error) {
	for _, fo := range fos {
		if strings.HasPrefix(fo.Name(), strconv.Itoa(version)) {
			return fo, nil
		}
	}
	return nil, fmt.Errorf(errMissingMigrationFileTpl, version)
}

func getMigrationsFiles(dir, direction string) ([]os.FileInfo, error) {
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var fos []os.FileInfo
	for _, fo := range files {
		if fo.IsDir() {
			continue
		}
		if !strings.HasSuffix(fo.Name(), fmt.Sprintf(".%s.sql", direction)) {
			continue
		}
		fos = append(fos, fo)
	}
	return fos, nil
}

func getMigratedVersions(url string) ([]int, error) {
	dbConn, err := dbr.Open("postgres", url, nil)
	if err != nil {
		return nil, err
	}
	sess := dbConn.NewSession(nil)
	var versions []int
	if _, err := sess.Select("version").From(migrationsTable).OrderDir("version", false).LoadValues(&versions); err != nil {
		return nil, err
	}
	return versions, nil
}

func getVersion(filename string) (int, error) {
	fs := strings.Split(filename, "_")
	return strconv.Atoi(fs[0])
}

func superSet(vs1, vs2 []int) (r1 []int) {
	for _, i := range vs1 {
		found := false
		for _, j := range vs2 {
			if i == j {
				found = true
				continue
			}
		}
		if !found {
			r1 = append(r1, i)
		}
	}
	return
}
