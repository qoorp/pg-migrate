package pgmigrate

import (
	"fmt"
	"strings"
)

type Logger interface {
	Printf(format string, args ...interface{})
	Warn(args ...interface{})
	Print(args ...interface{})
	Error(args ...interface{})
	Inf(args ...interface{})
	DBG(args ...interface{})
	Ok(args ...interface{})
}

type defaultLogger struct{}

func (l *defaultLogger) Printf(format string, args ...interface{}) {
	fmt.Printf(format, args...)
}

func printFEach(logger Logger, format string, args ...interface{}) {
	for _, v := range args {
		logger.Printf(format, v)
	}
}

func (l *defaultLogger) Warn(args ...interface{}) {
	printFEach(l, "WARN: %v\n", args...)
}

func (l *defaultLogger) Print(args ...interface{}) {
	printFEach(l, "%v\n", args...)
}

func (l *defaultLogger) Error(args ...interface{}) {
	printFEach(l, "ERROR: %v\n", args...)
}

func (l *defaultLogger) Inf(args ...interface{}) {
	printFEach(l, "INF: %v\n", args...)
}

func (l *defaultLogger) DBG(args ...interface{}) {
	printFEach(l, "DBG: [%v]\n", args...)
}

func (l *defaultLogger) Ok(args ...interface{}) {
	l.Print(args...)
}

func (ctx *PGMigrate) dbg(lbl string, args ...interface{}) {
	if ctx.config.Debug {
		if len(args) == 0 {
			ctx.logger.DBG(fmt.Sprintf("[%s]", lbl))
			return
		}
		ctx.logger.DBG(args...)
		for _, v := range args {
			ctx.logger.DBG(fmt.Sprintf("[%s] %v", lbl, v))
		}
	}
}

func (ctx *PGMigrate) dbgJoin(lbl string, args ...string) {
	if ctx.config.Debug {
		if len(args) == 0 {
			ctx.logger.DBG(lbl)
			return
		}
		ctx.logger.DBG(fmt.Sprintf(lbl+" %s", strings.Join(args, " ")))
	}
}
