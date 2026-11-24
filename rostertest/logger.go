package rostertest

import (
	"fmt"
	"sync"

	"github.com/davidbirdsong/roster"
)

// Entry is one line a Logger received.
type Entry struct {
	Level string
	Msg   string
	// Fields are the key and value pairs, keyed by the string form of the key.
	Fields map[string]any
}

// Logger records what a library logs. It is not an slog.Logger, so it proves the library needs nothing more
// than roster.Logger.
type Logger struct {
	mu      sync.Mutex
	entries []Entry
}

var _ roster.Logger = (*Logger)(nil)

func (l *Logger) Info(msg string, args ...any)  { l.add("info", msg, args) }
func (l *Logger) Warn(msg string, args ...any)  { l.add("warn", msg, args) }
func (l *Logger) Error(msg string, args ...any) { l.add("error", msg, args) }

func (l *Logger) add(level, msg string, args []any) {
	fields := map[string]any{}
	for i := 0; i+1 < len(args); i += 2 {
		fields[fmt.Sprint(args[i])] = args[i+1]
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.entries = append(l.entries, Entry{Level: level, Msg: msg, Fields: fields})
}

// Entries returns a copy of what was logged so far.
func (l *Logger) Entries() []Entry {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]Entry(nil), l.entries...)
}
