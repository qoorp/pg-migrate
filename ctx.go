package pgmigrate

import (
	"github.com/gocraft/dbr"
)

type PGMigrate struct {
	dbConn *dbr.Connection
	tx     *dbr.Tx
	config Config
	logger Logger
}

func ctxNew(config Config) *PGMigrate {
	ctx := &PGMigrate{}
	if config.Logger == nil {
		ctx.logger = &defaultLogger{}
		ctx.dbg("ctxNew", "no logger provided, using default")
	} else {
		ctx.logger = config.Logger
	}
	if config.MigrationsTable == "" {
		ctx.dbg("ctxNew", "no migrations table provided, using default")
		config.MigrationsTable = defaultMigrationsTable
	}
	ctx.config = config
	return ctx
}

func (ctx *PGMigrate) finish() error {
	ctx.dbg("finish")
	if err := ctx.dbFinish(); err != nil {
		ctx.dbg("finish", err)
		return err
	}
	return nil
}
