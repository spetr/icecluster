package logsink

import (
	"io"
	"log"
	"os"
)

var (
	filePath string
	outFile  *os.File
	verbose  bool
)

// Configure sets the log output destination and verbosity.
// If path is empty, logs go to stdout and Reopen is a no-op.
func Configure(path string, v bool) error {
	verbose = v
	filePath = path
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	if path == "" {
		// stdout
		if outFile != nil {
			_ = outFile.Close()
			outFile = nil
		}
		log.SetOutput(os.Stdout)
		return nil
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	if outFile != nil {
		_ = outFile.Close()
	}
	outFile = f
	log.SetOutput(outFile)
	return nil
}

// Reopen closes and reopens the log file to support external rotation.
func Reopen() error {
	if filePath == "" {
		// stdout; nothing to reopen
		return nil
	}
	f, err := os.OpenFile(filePath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	if outFile != nil {
		_ = outFile.Close()
	}
	outFile = f
	log.SetOutput(outFile)
	return nil
}

// Writer returns the current writer (useful for tests).
func Writer() io.Writer {
	if outFile != nil {
		return outFile
	}
	return os.Stdout
}

// Verbose indicates whether verbose logging is enabled.
func Verbose() bool { return verbose }

// Vprintf logs if verbose is enabled.
func Vprintf(format string, args ...any) {
	if verbose {
		log.Printf(format, args...)
	}
}
