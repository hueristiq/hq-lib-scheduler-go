package logger

// Nop is a [Logger] that discards every record; it is the scheduler's
// default logger.
type Nop struct{}

// Debug discards the record.
//
// Parameters:
//   - msg (string): ignored.
//   - args (...any): ignored.
func (Nop) Debug(msg string, args ...any) {}

// Info discards the record.
//
// Parameters:
//   - msg (string): ignored.
//   - args (...any): ignored.
func (Nop) Info(msg string, args ...any) {}

// Warn discards the record.
//
// Parameters:
//   - msg (string): ignored.
//   - args (...any): ignored.
func (Nop) Warn(msg string, args ...any) {}

// Error discards the record.
//
// Parameters:
//   - msg (string): ignored.
//   - args (...any): ignored.
func (Nop) Error(msg string, args ...any) {}

// Nop satisfies the contract.
var _ Logger = Nop{}
