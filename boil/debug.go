package boil

import (
	"context"
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"
)

// DebugMode is a flag controlling whether generated sql statements and
// debug information is outputted to the DebugWriter handle
//
// NOTE: This should be disabled in production to avoid leaking sensitive data
var DebugMode = false

// DebugWriter is where the debug output will be sent if DebugMode is true
var DebugWriter io.Writer = os.Stdout

// WithDebug modifies a context to configure debug writing. If true,
// all queries made using this context will be outputted to the io.Writer
// returned by DebugWriterFrom.
func WithDebug(ctx context.Context, debug bool) context.Context {
	return context.WithValue(ctx, ctxDebug, debug)
}

// IsDebug returns true if the context has debugging enabled, or
// the value of DebugMode if not set.
func IsDebug(ctx context.Context) bool {
	debug, ok := ctx.Value(ctxDebug).(bool)
	if ok {
		return debug
	}
	return DebugMode
}

// WithDebugWriter modifies a context to configure the writer written to
// when debugging is enabled.
func WithDebugWriter(ctx context.Context, writer io.Writer) context.Context {
	return context.WithValue(ctx, ctxDebugWriter, writer)
}

// DebugWriterFrom returns the debug writer for the context, or DebugWriter
// if not set.
func DebugWriterFrom(ctx context.Context) io.Writer {
	writer, ok := ctx.Value(ctxDebugWriter).(io.Writer)
	if ok {
		return writer
	}
	return DebugWriter
}

// PrintQuery prints a modified query string with placeholders replaced by their
// corresponding argument values to writer.
func PrintQuery(writer io.Writer, query string, args ...interface{}) {
	substitutedQuery := substituteQueryArgs(query, args...)
	fmt.Fprintln(writer, substitutedQuery)
}

// substituteQueryArgs takes a query string and an array of arguments.
// It returns a modified query string with placeholders replaced by their
// corresponding argument values.
func substituteQueryArgs(query string, args ...interface{}) string {
	// find all occurrences of placeholders ($1, $2, etc.) in the query
	re := regexp.MustCompile(`\$\d+`)
	matches := re.FindAllString(query, -1)

	for i, match := range matches {
		var arg string

		switch v := args[i].(type) {
		case string:
			arg = fmt.Sprintf("'%s'", v)
		case []byte:
			arg = fmt.Sprintf("'%s'", string(v))
		default:
			arg = fmt.Sprintf("%v", v)
		}

		// replace the placeholder with the argument value
		query = strings.Replace(query, match, arg, 1)
	}

	// return the final query with argument values
	return query
}
