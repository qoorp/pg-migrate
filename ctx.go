package pqmigrate

import (
	"strings"

	"github.com/Preciselyco/pqdbr"
	"github.com/lib/pq"
)

// PQMigrate utility for managing common postgresql operations
type PQMigrate struct {
	dbConn   *pqdbr.Connection
	tx       *pqdbr.Tx
	dbTokens map[string]string
	config   Config
	logger   Logger
}

func ctxNew(config Config) *PQMigrate {
	ctx := &PQMigrate{}
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
	ctx.dbTokens = map[string]string{}
	if tokenStr, err := pq.ParseURL(config.DBUrl); err == nil {
		for _, tp := range strings.Split(tokenStr, " ") {
			tokens := strings.Split(tp, "=")
			if len(tokens) != 2 {
				continue
			}
			ctx.dbTokens[tokens[0]] = tokens[1]
		}
	}
	ctx.config = config
	return ctx
}

func (ctx *PQMigrate) finish() error {
	ctx.dbg("finish")
	if err := ctx.dbFinish(); err != nil {
		ctx.dbg("finish", err)
		return err
	}
	return nil
}
