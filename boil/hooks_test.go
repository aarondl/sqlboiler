package boil

import (
	"context"
	"testing"
)

func TestSkipHooks(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	if HooksAreSkipped(ctx) {
		t.Error("they should not be skipped")
	}

	ctx = SkipHooks(ctx)

	if !HooksAreSkipped(ctx) {
		t.Error("they should be skipped")
	}
}

func TestSkipTimestamps(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	if TimestampsAreSkipped(ctx) {
		t.Error("they should not be skipped")
	}

	ctx = SkipTimestamps(ctx)

	if !TimestampsAreSkipped(ctx) {
		t.Error("they should be skipped")
	}
}

func TestHooksAreSkippedWrongType(t *testing.T) {
	t.Parallel()

	ctx := context.WithValue(context.Background(), ctxSkipHooks, "yes")
	if HooksAreSkipped(ctx) {
		t.Error("expected false when context value is wrong type")
	}
}

func TestTimestampsAreSkippedWrongType(t *testing.T) {
	t.Parallel()

	ctx := context.WithValue(context.Background(), ctxSkipTimestamps, "yes")
	if TimestampsAreSkipped(ctx) {
		t.Error("expected false when context value is wrong type")
	}
}
