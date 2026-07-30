package logger

// Logger is the logging contract of the scheduler: four leveled methods,
// each taking a message and alternating keys and values annotating the
// record.
//
// Implementations must be safe for concurrent use: the scheduler calls the
// logger from its scheduling loop and from per-run goroutines. A panic from
// any method is recovered by the scheduler — a misbehaving implementation
// cannot crash it; the panicking record is dropped.
type Logger interface {
	// Debug logs a fine-grained diagnostic record.
	//
	// Parameters:
	//   - msg (string): the log message.
	//   - args (...any): alternating keys and values annotating the record.
	Debug(msg string, args ...any)

	// Info logs an informational record.
	//
	// Parameters:
	//   - msg (string): the log message.
	//   - args (...any): alternating keys and values annotating the record.
	Info(msg string, args ...any)

	// Warn logs a warning record.
	//
	// Parameters:
	//   - msg (string): the log message.
	//   - args (...any): alternating keys and values annotating the record.
	Warn(msg string, args ...any)

	// Error logs a failure record.
	//
	// Parameters:
	//   - msg (string): the log message.
	//   - args (...any): alternating keys and values annotating the record.
	Error(msg string, args ...any)
}
