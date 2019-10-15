package pgmigrate

import (
	"golang.org/x/text/unicode/norm"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
)

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
	if _, err := os.Stat(ctx.config.BaseDirectory); os.IsNotExist(err) {
		ctx.dbg("fileWriteContents", "base directory does not exist. creating.")
		if err := os.MkdirAll(ctx.config.BaseDirectory, os.ModeDir); err != nil {
			ctx.dbg("fileWriteContents", err)
			return err
		}
		ctx.dbg("fileWriteContents", "ok")
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
