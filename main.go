package main

import (
	"bytes"
	"fmt"
	"github.com/docopt/docopt-go"
	"github.com/gocraft/dbr"
	"github.com/happierall/l"
	"github.com/joho/godotenv"
	_ "github.com/lib/pq"
	"golang.org/x/text/unicode/norm"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

const migrationsTable = "pgmigrate"

var (
	errMissingMigrationFilesTpl = "there are missing migration files: %v"
	errMissingMigrationFileTpl  = "missing migration file with version: %v"
	reMigrationName             = regexp.MustCompile("^[a-z0-9_]+$")
)

func main() {
	usage := `pg-migrate

Usage:
  pg-migrate up [--url=<url>] [--dir=<dir>] [--steps=<steps>] [--bw]
  pg-migrate down [--url=<url>] [--dir=<dir>] [--steps=<steps>] [--bw]
  pg-migrate create <name> [--bw]
  pg-migrate dump
  pg-migrate load [--dir=<dir>]
  pg-migrate seed [--dir=<dir>]
  pg-migrate -h | --help
  pg-migrate --version

Options:
  -h --help        Show help.
  --version        Show version.
  --dir=<dir>      Directory where migrations files are stores. [default: pgmigrate/]
  --steps=<steps>  Max steps to migrate [default: 1].
  --bw        No colour (black and white).
`
	err := godotenv.Load()
	if err != nil {
		l.Warn("no .env file")
	}
	arguments, _ := docopt.Parse(usage, nil, true, "pg-migrate", false)
	if arguments["--bw"].(bool) {
		l.Default.Production = true
	}
	if arguments["up"].(bool) {
		l.Print("migrating up...")
		url, fullDir, steps, err := getMigrateArgs(arguments)
		if err == nil {
			if err := upCMD(url, fullDir, steps); err != nil {
				l.Error(err)
				return
			}
		} else {
			l.Error(err)
			return
		}
	} else if arguments["down"].(bool) {
		l.Print("migrating down...")
		url, fullDir, steps, err := getMigrateArgs(arguments)
		if err == nil {
			if err := downCMD(url, fullDir, steps); err != nil {
				l.Error(err)
				return
			}
		} else {
			l.Error(err)
			return
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
		} else {
			l.Error(err)
			return
		}
	} else if arguments["dump"].(bool) {
		l.Print("dumping sql...")
		url, dir, _, err := getMigrateArgs(arguments)
		if err == nil {
			if err := dumpCMD(url, dir); err != nil {
				l.Error(err)
				return
			}
		} else {
			l.Error(err)
			return
		}
	} else if arguments["load"].(bool) {
		l.Print("loading sql...")
		url, dir, _, err := getMigrateArgs(arguments)
		if err != nil {
			l.Error(err)
			return
		}
		if err := loadCMD(url, dir); err != nil {
			l.Error(err)
			return
		}
	} else if arguments["seed"].(bool) {
		l.Print("seeding db...")
		url, dir, _, err := getMigrateArgs(arguments)
		if err != nil {
			l.Error(err)
			return
		}
		if err := seedCMD(url, dir); err != nil {
			l.Error(err)
			return
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
	url := os.Getenv("DATABASE_URL")
	if u, found := arguments["<url>"]; found {
		url = u.(string)
	}
	if url == "" {
		return "", "", 0, fmt.Errorf("no url provided")
	}
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
	migrations, err := getAllMigrations(dir)
	if err != nil {
		return err
	}
	err = migrationsTableExist(url)
	if err != nil {
		return err
	}
	migratedVersions, err := getMigratedVersions(url)
	if err != nil {
		return err
	}
	ss2 := superSet(migrations, migratedVersions)
	if len(ss2) == 0 {
		l.Print("there was nothing to migrate")
	}
	stepsLeft := steps
	for _, m := range ss2 {
		if stepsLeft < 1 {
			break
		}
		if err := doMigrate(url, m, true); err != nil {

		}
		stepsLeft--
	}
	return nil
}

func downCMD(url, dir string, steps int) error {
	if err := migrationsTableExist(url); err != nil {
		return err
	}
	migratedVersions, err := getMigratedVersions(url)
	if err != nil {
		return err
	}
	stepsLeft := steps
	for _, v := range migratedVersions {
		if stepsLeft < 1 {
			break
		}
		if err := doMigrate(url, v, false); err != nil {
			return err
		}
		stepsLeft--
	}
	return nil
}

func dumpCMD(url, dir string) error {
	cmd := exec.Command("pg_dump", url, "-s", "-O")
	var out bytes.Buffer
	cmd.Stdout = &out
	if err := cmd.Run(); err != nil {
		return err
	}
	outFilePath := filepath.Join(dir, fmt.Sprintf("dump_%d.sql", time.Now().Unix()))
	l.Printf("writing dump to %s", outFilePath)
	err := ioutil.WriteFile(outFilePath, out.Bytes(), 0644)
	if err != nil {
		return err
	}
	return nil
}

func getFileContents(dir, fileName string) (string, error) {
	cb, err := ioutil.ReadFile(filepath.Join(dir, fileName))
	if err != nil {
		return "", err
	}
	return norm.NFC.String(strings.TrimSpace(string(cb))), nil
}

func loadCMD(url, dir string) error {
	l.Printf("loading > %s/schema.sql", dir)
	schema, err := getFileContents(dir, "schema.sql")
	if err != nil {
		return err
	}
	return execString(url, schema, nil)
}

func seedCMD(url, dir string) error {
	l.Printf("loading > %s/seeds.sql", dir)
	seeds, err := getFileContents(dir, "seeds.sql")
	if err != nil {
		return err
	}
	return execString(url, seeds, nil)
}

func execString(url, contents string, cb func(tx *dbr.Tx) error) error {
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

	if _, err := tx.Exec(string(contents)); err != nil {
		return err
	}
	if cb != nil {
		if err := cb(tx); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func doMigrate(url string, mig *migration, migrateUp bool) error {
	l.Printf("migrating > %s", mig.Name)
	contents := mig.Up
	if !migrateUp {
		contents = mig.Down
	}
	return execString(url, contents, func(tx *dbr.Tx) error {
		if migrateUp {
			if _, err := tx.InsertInto(migrationsTable).
				Columns("version", "name", "up", "down").
				Values(mig.Version, mig.Name, mig.Up, mig.Down).
				Exec(); err != nil {
				return err
			}
		} else {
			if _, err := tx.DeleteFrom(migrationsTable).
				Where(dbr.Eq("version", mig.Version)).
				Exec(); err != nil {
				return err
			}
		}
		return nil
	})
}

func getCurrentMigrationFiles(version int, fos []os.FileInfo) []os.FileInfo {
	files := []os.FileInfo{}
	for _, fo := range fos {
		if strings.HasPrefix(fo.Name(), strconv.Itoa(version)) {
			files = append(files, fo)
		}
	}
	return files
}

func getAllMigrations(dir string) ([]*migration, error) {
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	reg := regexp.MustCompilePOSIX("^.*(up|down).sql$")
	migMap := map[uint64]*migration{}
	for _, fo := range files {
		if fo.IsDir() {
			continue
		}
		sm := reg.FindStringSubmatch(fo.Name())
		if len(sm) != 2 {
			continue
		}
		direction := sm[1]
		version, err := getVersion(fo.Name())
		if err != nil {
			return nil, err
		}
		contents, err := getFileContents(dir, fo.Name())
		migNameParts := strings.Split(fo.Name(), ".")
		migrationName := strings.Join(migNameParts[0:len(migNameParts)-2], ".")
		if m, found := migMap[version]; found {
			if direction == "up" {
				m.Up = contents
			} else {
				m.Down = contents
			}
		} else {
			m := &migration{
				Version: version,
				Name:    migrationName,
			}
			if direction == "up" {
				m.Up = contents
			} else {
				m.Down = contents
			}
			migMap[version] = m
		}
	}
	migrations := []*migration{}
	for _, m := range migMap {
		migrations = append(migrations, m)
	}
	sort.Sort(ByVersion(migrations))
	return migrations, nil
}

type migration struct {
	Version uint64 `db:"version"`
	Name    string `db:"name"`
	Up      string `db:"up"`
	Down    string `db:"down"`
}

type ByVersion []*migration

func (a ByVersion) Len() int           { return len(a) }
func (a ByVersion) Less(i, j int) bool { return a[i].Version < a[j].Version }
func (a ByVersion) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }

func getMigratedVersions(url string) ([]*migration, error) {
	dbConn, err := dbr.Open("postgres", url, nil)
	if err != nil {
		return nil, err
	}
	sess := dbConn.NewSession(nil)
	migrations := []*migration{}
	if _, err := sess.Select("*").From(migrationsTable).OrderDir("version", false).Load(&migrations); err != nil {
		return nil, err
	}
	return migrations, nil
}

func getVersion(filename string) (uint64, error) {
	fs := strings.Split(filename, "_")
	return strconv.ParseUint(fs[0], 10, 64)
}

// Create the table if it does not exist.
func migrationsTableExist(url string) error {
	dbConn, err := dbr.Open("postgres", url, nil)
	if err != nil {
		return err
	}
	sess := dbConn.NewSession(nil)
	s := fmt.Sprintf(`create table if not exists %s (
			version bigint not null primary key,
			name text not null default '',
			up text not null default '',
			down text not null default ''
		)`, migrationsTable)
	_, err = sess.Exec(s)
	return err
}

func superSet(vs1, vs2 []*migration) (r1 []*migration) {
	for _, i := range vs1 {
		found := false
		for _, j := range vs2 {
			if i.Version == j.Version {
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
