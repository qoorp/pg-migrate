package pqmigrate

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"

	"golang.org/x/text/unicode/norm"
)

func (ctx *PGMigrate) fileEnsureDirExist(path string) error {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		ctx.dbg("fileEnsureDirExists", fmt.Sprintf("directory '%s' does not exist, creating", path))
		if err := os.MkdirAll(path, 0755); err != nil {
			ctx.dbg("fileEnsureExists", err)
			return err
		}
	}
	return nil
}

func (ctx *PGMigrate) fileGetContents(fileName string) (string, error) {
	fp := filepath.Join(ctx.config.BaseDirectory, fileName)
	ctx.dbgJoin("fileGetContents", "getting:", fp)
	cb, err := ioutil.ReadFile(fp)
	if err != nil {
		ctx.dbg("fileGetContents", err)
		return "", err
	}
	return norm.NFC.String(strings.TrimSpace(string(cb))), nil
}

func (ctx *PGMigrate) fileWriteContents(fileName string, contents []byte) error {
	fp := filepath.Join(ctx.config.BaseDirectory, fileName)
	ctx.dbg("fileWriteContents", fp)
	if err := ctx.fileEnsureDirExist(ctx.config.BaseDirectory); err != nil {
		return err
	}
	return ioutil.WriteFile(fp, contents, 0644)
}

func (ctx *PGMigrate) fileExec(fileName string) error {
	ctx.dbg("fileExec", fileName)
	contents, err := ctx.fileGetContents(fileName)
	if err != nil {
		ctx.dbg("fileExec", err)
		return err
	}
	return ctx.dbExecString(contents, nil)
}
