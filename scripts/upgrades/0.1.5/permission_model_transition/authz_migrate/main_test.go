// Copyright openbkn.ai
//
// Licensed under the OpenBKN License. See LICENSE-OPENBKN.txt in the project root.

package main

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"
)

func TestExecuteRejectsUnknownModeBeforeLoadingAnyRuntimeConfiguration(t *testing.T) {
	if _, err := execute(context.Background(), "unknown", "", time.Now()); err == nil || !strings.Contains(err.Error(), "unsupported mode") {
		t.Fatalf("unknown mode error = %v", err)
	}
}

func TestWriteReportProducesReviewableJSON(t *testing.T) {
	var output bytes.Buffer
	report := commandReport{Mode: modeDryRun, GeneratedAt: time.Date(2026, 9, 11, 8, 0, 0, 0, time.UTC)}
	if err := writeReport(&output, report); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{`"mode": "dry-run"`, `"core"`, `"enterprise"`} {
		if !strings.Contains(output.String(), want) {
			t.Fatalf("report missing %s: %s", want, output.String())
		}
	}
}
