package pqmigrate

import (
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

var (
	reMigrationName   = regexp.MustCompilePOSIX(`^[a-z0-9][a-z0-9_]+$`)
	migrationFileRegx = regexp.MustCompilePOSIX(`^[0-9]{10}[^.]+\.(up|down).sql$`)
	migrationRegx     = regexp.MustCompilePOSIX(`^([0-9]{10}[^.]+)`)
)

type byVersion []*migration

func (a byVersion) Len() int           { return len(a) }
func (a byVersion) Less(i, j int) bool { return a[i].Version < a[j].Version }
func (a byVersion) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }

type byVersionReversed []*migration

func (a byVersionReversed) Len() int           { return len(a) }
func (a byVersionReversed) Less(i, j int) bool { return a[j].Version < a[i].Version }
func (a byVersionReversed) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }

func (ctx *PQMigrate) migrationGetVersion(fileName string) (uint64, error) {
	ctx.dbg("migrationGetVersion", fileName)
	fs := strings.Split(fileName, "_")
	return strconv.ParseUint(fs[0], 10, 64)
}

var errNotMigrationFile = fmt.Errorf("not a migration file")

func (ctx *PQMigrate) migrationGetSpecific(fileName string) (*migration, error) {
	ctx.dbg("migrationGetSpecific", fileName)
	sm := migrationFileRegx.FindStringSubmatch(fileName)
	if len(sm) != 2 {
		return nil, errNotMigrationFile
	}
	ctx.dbg("migrationGetSpecific", sm)
	direction := sm[1]
	version, err := ctx.migrationGetVersion(fileName)
	if err != nil {
		ctx.dbg("migrationGetSpecific", err)
		return nil, err
	}
	var upContents string
	var downContents string
	var upFileName string
	var downFileName string
	migrationName := migrationRegx.FindString(fileName)
	if migrationName == "" {
		return nil, errNotMigrationFile
	}
	if direction == "up" {
		upFileName = fileName
		downFileName = migrationName + ".down.sql"
	} else {
		downFileName = fileName
		upFileName = migrationName + ".up.sql"
	}
	upContents, err = ctx.fileGetContents(upFileName)
	if err != nil {
		return nil, err
	}
	downContents, err = ctx.fileGetContents(downFileName)
	if err != nil {
		return nil, err
	}
	return &migration{
		Name:    migrationName,
		Version: version,
		Up:      upContents,
		Down:    downContents,
	}, nil
}

func (ctx *PQMigrate) migrationGetAll() ([]*migration, error) {
	ctx.dbg("migrationGetAll")
	files, err := ioutil.ReadDir(ctx.config.BaseDirectory)
	if err != nil {
		ctx.dbg("migrationGetAll", err)
		return nil, err
	}
	migMap := map[uint64]*migration{}
	for _, fo := range files {
		if fo.IsDir() {
			continue
		}
		sm := migrationFileRegx.FindStringSubmatch(fo.Name())
		if len(sm) != 2 {
			continue
		}
		ctx.dbg("migrationGetAll", sm)
		version, err := ctx.migrationGetVersion(fo.Name())
		if err != nil {
			ctx.dbg("migrationGetAll", err)
			return nil, err
		}
		if _, found := migMap[version]; found {
			continue
		}
		m, err := ctx.migrationGetSpecific(fo.Name())
		if err != nil {
			return nil, err
		}
		migMap[m.Version] = m
	}
	migrations := make([]*migration, 0)
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
