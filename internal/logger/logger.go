package logger

import (
	"io"
	"os"
	"strings"

	"github.com/sirupsen/logrus"
)

// Fields defines a shorthand for structured logging fields.
type Fields map[string]any

// Logger wraps a logrus.Entry and provides structured logging methods.
type Logger struct {
	entry *logrus.Entry
}

// initBaseLogger configures the base logger instance with output, formatting, and log level.
func initBaseLogger(out io.Writer) *logrus.Logger {
	log := logrus.New()
	if out == nil {
		out = os.Stdout
	}
	log.SetOutput(out)
	log.SetFormatter(&logrus.TextFormatter{
		FullTimestamp:   true,
		TimestampFormat: "2006-01-02 15:04:05",
		ForceColors:     true,
	})
	level, err := logrus.ParseLevel(strings.ToLower(os.Getenv("LOG_LEVEL")))
	if err != nil {
		level = logrus.InfoLevel
	}
	log.SetLevel(level)
	return log
}

// New creates a new Logger instance wrapping the default logger
// with configured output, formatting, and log level.
// If output is nil, io.Stdout is used.
func New(output io.Writer) *Logger {
	base := initBaseLogger(output)
	return &Logger{entry: logrus.NewEntry(base)}
}

// WithFields returns a new log entry with the given structured fields attached.
func (l *Logger) WithFields(f Fields) *logrus.Entry {
	return l.entry.WithFields(logrus.Fields(f))
}

// WithField returns a new log entry with a single field attached.
func (l *Logger) WithField(key string, value any) *logrus.Entry {
	return l.entry.WithField(key, value)
}

// WithError returns a new log entry with the given error attached.
func (l *Logger) WithError(err error) *logrus.Entry {
	return l.entry.WithError(err)
}

// Info logs a message at the Info level.
func (l *Logger) Info(args ...any) { l.entry.Info(args...) }

// Error logs a message at the Error level.
func (l *Logger) Error(args ...any) { l.entry.Error(args...) }

// Error logs a message at the Debug level.
func (l *Logger) Debug(args ...any) { l.entry.Debug(args...) }

// Error logs a message at the Warn level.
func (l *Logger) Warn(args ...any) { l.entry.Warn(args...) }

// Error logs a message at the Panic level and then panics.
func (l *Logger) Panic(args ...any) { l.entry.Panic(args...) }

// Fatal logs a message at the Fatal level and then exits the application.
func (l *Logger) Fatal(args ...any) { l.entry.Fatal(args...) }

// Infof logs a formatted message at the Info level.
func (l *Logger) Infof(format string, args ...any) { l.entry.Infof(format, args...) }

// Errorf logs a formatted message at the Error level.
func (l *Logger) Errorf(format string, args ...any) { l.entry.Errorf(format, args...) }

// Debugf logs a formatted message at the Debug level.
func (l *Logger) Debugf(format string, args ...any) { l.entry.Debugf(format, args...) }

// Warnf logs a formatted message at the Warn level.
func (l *Logger) Warnf(format string, args ...any) { l.entry.Warnf(format, args...) }

// Panicf logs a formatted message at the Panic level and then panics.
func (l *Logger) Panicf(format string, args ...any) { l.entry.Panicf(format, args...) }

// Fatalf logs a formatted message at the Fatal level and then exits the application.
func (l *Logger) Fatalf(format string, args ...any) { l.entry.Fatalf(format, args...) }
