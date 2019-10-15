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
	printFEach(l, "WARN: %v", args...)
}

func (l *defaultLogger) Print(args ...interface{}) {
	printFEach(l, "%v", args...)
}

func (l *defaultLogger) Error(args ...interface{}) {
	printFEach(l, "ERROR: %v", args...)
}

func (ctx *PGMigrate) dbg(lbl string, args ...interface{}) {
	if ctx.config.Debug {
		if len(args) == 0 {
			ctx.logger.Printf("DBG: [%s]", lbl)
			return
		}
		printFEach(ctx.logger, fmt.Sprintf("DBG: [%s]", lbl)+" %v", args...)
	}
}

func (ctx *PGMigrate) dbgJoin(lbl string, args ...string) {
	if ctx.config.Debug {
		if len(args) == 0 {
			ctx.logger.Printf("DBG: [%s]", lbl)
			return
		}
		ctx.logger.Printf(fmt.Sprintf("DBG: [%s]", lbl)+" %s", strings.Join(args, " "))
	}
}
