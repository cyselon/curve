package log

import (
	"log/slog"
	"strings"
)

// LevelWrapper 包装了 slog.LevelVar，使其符合 pflag.Value 接口
type LevelWrapper struct {
	LevelVar *slog.LevelVar
}

func (l *LevelWrapper) String() string {
	return l.LevelVar.Level().String()
}

func (l *LevelWrapper) Set(s string) error {
	var lvl slog.Level
	// UnmarshalText 支持 "DEBUG", "INFO" 等字符串
	err := lvl.UnmarshalText([]byte(strings.ToUpper(s)))
	if err != nil {
		return err
	}
	l.LevelVar.Set(lvl)
	return nil
}

func (l *LevelWrapper) Type() string {
	return "string"
}
