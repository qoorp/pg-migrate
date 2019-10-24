package pqmigrate

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

type migration struct {
	Version uint64 `db:"version"`
	Name    string `db:"name"`
	Up      string `db:"up"`
	Down    string `db:"down"`
}

var reMigrationName = regexp.MustCompilePOSIX("^[a-z0-9][a-z0-9_]+$")

type byVersion []*migration

func (a byVersion) Len() int           { return len(a) }
func (a byVersion) Less(i, j int) bool { return a[i].Version < a[j].Version }
func (a byVersion) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }

func (m *migration) UnmarshalJSON(b []byte) error {
	type alias migration
	mm := &alias{}
	if err := json.Unmarshal(b, mm); err != nil {
		return err
	}
	if b, err := base64.StdEncoding.DecodeString(mm.Up); err == nil {
		mm.Up = string(b)
	} else {
		return err
	}
	if b, err := base64.StdEncoding.DecodeString(mm.Down); err == nil {
		mm.Down = string(b)
	} else {
		return err
	}
	m.Version = mm.Version
	m.Name = mm.Name
	m.Up = mm.Up
	m.Down = mm.Down
	return nil
}

func (m *migration) MarshalJSON() ([]byte, error) {
	type alias migration
	return json.Marshal(&struct {
		Up   string `json:"up"`
		Down string `json:"down"`
		*alias
	}{
		Up:    base64.StdEncoding.EncodeToString([]byte(m.Up)),
		Down:  base64.StdEncoding.EncodeToString([]byte(m.Down)),
		alias: (*alias)(m),
	})
}

func (ctx *PQMigrate) migrationGetVersion(fileName string) (uint64, error) {
	ctx.dbg("migrationGetVersion", fileName)
	fs := strings.Split(fileName, "_")
	return strconv.ParseUint(fs[0], 10, 64)
}

func (ctx *PQMigrate) migrationGetAll() ([]*migration, error) {
	ctx.dbg("migrationGetAll")
	files, err := ioutil.ReadDir(ctx.config.BaseDirectory)
	if err != nil {
		ctx.dbg("migrationGetAll", err)
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
		ctx.dbg("migrationGetAll", sm)
		direction := sm[1]
		version, err := ctx.migrationGetVersion(fo.Name())
		if err != nil {
			ctx.dbg("migrationGetAll", err)
			return nil, err
		}
		contents, err := ctx.fileGetContents(fo.Name())
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
	ctx.dbg("migrationGetAll", "sorting migrations")
	sort.Sort(byVersion(migrations))
	return migrations, nil
}

func (ctx *PQMigrate) migrationSuperSet(vs1, vs2 []*migration) (r1 []*migration) {
	ctx.dbg("migrationSuperSet")
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

func (ctx *PQMigrate) migrationCreate(name string) error {
	ctx.dbg("migrationCreate", name)
	if ok := reMigrationName.MatchString(name); !ok {
		return fmt.Errorf("invalid migration name, must match the regexp: ^[a-z0-9_]+$")
	}
	epoch := strconv.FormatInt(time.Now().Unix(), 10)
	down := epoch + "_" + name + ".down.sql"
	up := epoch + "_" + name + ".up.sql"
	ctx.logger.Print(fmt.Sprintf("creating %s", down))
	if err := ctx.fileWriteContents(down, []byte("")); err != nil {
		return err
	}
	ctx.logger.Print(fmt.Sprintf("creating %s", up))
	return ctx.fileWriteContents(up, []byte(""))
}

type migrationSet struct {
	items map[uint64]*migration
}

func newMigrationSet() *migrationSet {
	return &migrationSet{
		items: make(map[uint64]*migration),
	}
}

func (s *migrationSet) add(items ...*migration) {
	for _, m := range items {
		s.items[m.Version] = m
	}
}

func (s *migrationSet) rem(items ...*migration) {
	for _, m := range items {
		delete(s.items, m.Version)
	}
}

func (s *migrationSet) has(item *migration) bool {
	_, found := s.items[item.Version]
	return found
}

func (s *migrationSet) itemsSlice() []*migration {
	rs := make([]*migration, 0)
	for _, m := range s.items {
		rs = append(rs, m)
	}
	sort.Sort(byVersion(rs))
	return rs
}

func migrationSliceIntersection(a, b []*migration) []*migration {
	sa := newMigrationSet()
	sb := newMigrationSet()
	sa.add(a...)
	for _, m := range b {
		if sa.has(m) {
			sb.add(m)
		}
	}
	items := sb.itemsSlice()
	sort.Sort(byVersion(items))
	return items
}

func migrationSliceUnion(a, b []*migration) []*migration {
	s := newMigrationSet()
	s.add(a...)
	s.add(b...)
	items := s.itemsSlice()
	sort.Sort(byVersion(items))
	return items
}

func migrationSliceDifference(a, b []*migration) []*migration {
	s := newMigrationSet()
	s.add(a...)
	s.rem(b...)
	items := s.itemsSlice()
	sort.Sort(byVersion(items))
	return items
}

func migrationSliceSymmetricDifference(a, b []*migration) []*migration {
	return migrationSliceUnion(migrationSliceDifference(a, b), migrationSliceDifference(b, a))
}
