package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/docopt/docopt-go"
	"github.com/gocraft/dbr"
	"github.com/happierall/l"
	_ "github.com/lib/pq"
)

const migrationsTable = "schema_migrations"

var (
	errMissingMigrationFilesTpl = "there are missing migration files: %v"
	errMissingMigrationFileTpl  = "missing migration file with version: %v"
	reMigrationName             = regexp.MustCompile("^[a-z0-9_]+$")
)

func main() {
	usage := `pg-migrate

Usage:
  pg-migrate up <url> [--dir=<dir>] [--steps=<steps>]
  pg-migrate down <url> [--dir=<dir>] [--steps=<steps>]
  pg-migrate create <name>
  pg-migrate -h | --help
  pg-migrate --version

Options:
  -h --help        Show help.
  --version        Show version.
  --dir=<dir>      Directory where migrations files are stores. [default: migrations/]
  --steps=<steps>  Max steps to migrate [default: 1].
`
	arguments, _ := docopt.Parse(usage, nil, true, "pg-migrate", false)
	if arguments["up"].(bool) {
		l.Print("migrating up...")
		url, fullDir, steps, err := getMigrateArgs(arguments)
		if err == nil {
			if err := upCMD(url, fullDir, steps); err != nil {
				l.Error(err)
				return
			}
		}
	} else if arguments["down"].(bool) {
		l.Print("migrating down...")
		url, fullDir, steps, err := getMigrateArgs(arguments)
		if err == nil {
			if err := downCMD(url, fullDir, steps); err != nil {
				l.Error(err)
				return
			}
		}
	} else if arguments["create"].(bool) {
		l.Print("creating new migration files...")
		fullDir, err := getFullDirArg(arguments)
		if err == nil {
			name := arguments["<name>"].(string)
			if err := createCMD(fullDir, name); err != nil {
				l.Error(err)
				return
			}
		}
	}
	l.Print("Success!")
}

func getFullDirArg(arguments map[string]interface{}) (string, error) {
	dir := arguments["--dir"].(string)
	var fullDir string
	var err error
	fullDir, err = filepath.Abs(dir)
	if err != nil {
		l.Error("error:", err)
	}
	return fullDir, err
}

func getMigrateArgs(arguments map[string]interface{}) (string, string, int, error) {
	url := arguments["<url>"].(string)
	fullDir, err := getFullDirArg(arguments)
	if err != nil {
		return "", "", 0, err
	}
	var steps int
	steps, err = strconv.Atoi(arguments["--steps"].(string))
	if err != nil {
		l.Error("error:", err)
	}
	return url, fullDir, steps, err
}

func createCMD(fullDir, name string) error {
	if ok := reMigrationName.MatchString(name); !ok {
		return fmt.Errorf("invalid migration name, must match the regexp: ^[a-z0-9_]+$")
	}
	epoch := strconv.FormatInt(time.Now().Unix(), 10)
	down := filepath.Join(fullDir, epoch+"_"+name+".down.sql")
	up := filepath.Join(fullDir, epoch+"_"+name+".up.sql")
	l.Print(fmt.Sprintf("%s", down))
	if err := ioutil.WriteFile(down, []byte(""), 0644); err != nil {
		return err
	}
	l.Print(fmt.Sprintf("%s", up))
	if err := ioutil.WriteFile(up, []byte(""), 0644); err != nil {
		return err
	}
	return nil
}

func upCMD(url, dir string, steps int) error {
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
	err = migrationsTableExist(url)
	if err != nil {
		return err
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
	if len(ss2) == 0 {
		l.Print("there was nothing othing to migrate")
	}
	for _, v := range ss2 {
		f, err := getMigrateFile(v, fos)
		if err != nil {
			return err
		}
		if err := doMigrate(url, dir, f, true); err != nil {
			return err
		}
	}
	return nil
}

func downCMD(url, dir string, steps int) error {
	fos, err := getMigrationsFiles(dir, "down")
	if err != nil {
		return err
	}
	_ = fos
	migratedVersions, err := getMigratedVersions(url)
	if err != nil {
		return err
	}
	stepsLeft := steps
	for _, v := range migratedVersions {
		if stepsLeft < 1 {
			break
		}
		f, err := getMigrateFile(v, fos)
		if err != nil {
			return err
		}
		if err := doMigrate(url, dir, f, false); err != nil {
			return err
		}
		stepsLeft--
	}
	return nil
}

func doMigrate(url, dir string, file os.FileInfo, migrateUp bool) error {
	l.Printf("migrating > %s", file.Name())
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
	if migrateUp {
		if _, err := tx.InsertInto(migrationsTable).Columns("version").Values(version).Exec(); err != nil {
			return err
		}
	} else {
		if _, err := tx.DeleteFrom(migrationsTable).Where(dbr.Eq("version", version)).Exec(); err != nil {
			return err
		}
	}
	return tx.Commit()
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

// Create the table if it does not exist.
func migrationsTableExist(url string) error {
	dbConn, err := dbr.Open("postgres", url, nil)
	if err != nil {
		return err
	}
	sess := dbConn.NewSession(nil)
	s := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
			version bigint NOT NULL
		)`, migrationsTable)
	_, err = sess.Exec(s)
	return err
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
