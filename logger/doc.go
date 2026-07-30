// Package logger defines the logging contract of the scheduler package, and
// provides the no-op implementation used by default.
//
// A [Logger] receives the structured records the scheduler emits at
// lifecycle, activation, and failure points: four leveled methods, each
// taking a message and alternating keys and values annotating the record.
// Keys are snake_case strings. An implementation is registered through the
// scheduler's WithLogger option.
//
// Implementations must be safe for concurrent use: the scheduler calls the
// logger from its scheduling loop and from per-run goroutines, and recovers
// a panic from any method — a misbehaving implementation cannot crash it.
// [Nop] discards every record and is the default when no logger is
// configured.
package logger
