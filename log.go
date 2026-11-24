package roster

// Logger is the logging the library needs. *slog.Logger satisfies it, and other loggers (zerolog, zap) need a
// small adapter. The args are alternating keys and values, and the library passes nothing else (no slog.Attr or
// slog.Service), so an adapter can treat them as plain pairs.
//
// A nil Logger given to a WithLogger option is ignored and the default, slog.Default(), stays. A typed nil, such
// as a nil *slog.Logger, is not detected and panics on the first log call.
type Logger interface {
	Info(msg string, args ...any)
	Warn(msg string, args ...any)
	Error(msg string, args ...any)
}
