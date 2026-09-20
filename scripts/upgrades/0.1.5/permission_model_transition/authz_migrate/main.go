// Copyright openbkn.ai
//
// Licensed under the OpenBKN License. See LICENSE-OPENBKN.txt in the project root.

// Command authz-migrate is the release-owned authorization step invoked by the
// target release's deploy migration entry. It always preflights Core and EE
// before the first write and emits a machine-readable JSON report.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/openbkn-ai/bkn-foundry/bkn-safe/server/migrationcontract"
	"github.com/openbkn-ai/bkn-foundry/deploy/permission-transition-0.1.5/authz-migrate/internal/authzmigration"
)

const (
	modeDryRun = "dry-run"
	modeApply  = "apply"
)

type commandReport struct {
	Mode        string                    `json:"mode"`
	GeneratedAt time.Time                 `json:"generated_at"`
	Core        authzmigration.CoreReport `json:"core"`
	Enterprise  authzmigration.EEReport   `json:"enterprise"`
	Marker      *migrationcontract.Marker `json:"marker,omitempty"`
	Error       string                    `json:"error,omitempty"`
}

func main() {
	mode := flag.String("mode", modeDryRun, "dry-run or apply")
	configPath := flag.String("config", "", "bkn-safe YAML config; normal SAFE_* environment resolution is used when empty")
	flag.Parse()

	now := time.Now().UTC()
	report, err := execute(context.Background(), *mode, *configPath, now)
	if err != nil {
		report.Error = err.Error()
	}
	if encodeErr := writeReport(os.Stdout, report); encodeErr != nil {
		fmt.Fprintln(os.Stderr, "encode migration report:", encodeErr)
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "authorization migration failed:", err)
		os.Exit(1)
	}
}

func execute(ctx context.Context, mode, configPath string, now time.Time) (commandReport, error) {
	report := commandReport{Mode: mode, GeneratedAt: now}
	if mode != modeDryRun && mode != modeApply {
		return report, fmt.Errorf("unsupported mode %q", mode)
	}
	cfg, err := loadDatabaseConfig(configPath)
	if err != nil {
		return report, fmt.Errorf("load bkn-safe config: %w", err)
	}
	db, err := openDatabase(cfg)
	if err != nil {
		return report, err
	}
	if sqlDB, sqlErr := db.DB(); sqlErr == nil {
		defer sqlDB.Close()
	}

	// Both inventories must pass before ApplyCore can make the first write.
	// Ownership cannot be inferred from a historical Casbin operation. Keep an
	// unproven Core rule as legacy rather than ask an operator to author a
	// migration manifest.
	report.Core, err = authzmigration.PlanCore(ctx, db, nil)
	if err != nil {
		return report, fmt.Errorf("Core preflight: %w", err)
	}
	// EE history is reconciled as inactive. Re-activation belongs to the
	// post-upgrade authorization workflow, not an upgrade-time text file.
	eeOptions := authzmigration.DefaultEEOptions(now)
	report.Enterprise, err = authzmigration.PlanEnterprise(ctx, db, eeOptions)
	if err != nil {
		return report, fmt.Errorf("Enterprise preflight: %w", err)
	}
	if mode == modeDryRun {
		return report, nil
	}
	if _, err := authzmigration.ApplyCore(ctx, db, nil); err != nil {
		return report, err
	}
	if _, err := authzmigration.ApplyEnterprise(ctx, db, eeOptions); err != nil {
		return report, err
	}
	report.Core, report.Enterprise, err = authzmigration.ReconciledReports(ctx, db, nil, eeOptions)
	if err != nil {
		return report, err
	}
	marker, err := authzmigration.BuildMarker(report.Core, report.Enterprise, nil, now)
	if err != nil {
		return report, err
	}
	if err := authzmigration.PersistMarker(ctx, db, marker); err != nil {
		return report, err
	}
	if !marker.Valid() {
		return report, errors.New("persisted authorization migration marker is invalid")
	}
	var persisted migrationcontract.Marker
	if err := db.WithContext(ctx).Where("version = ?", migrationcontract.CurrentVersion).
		First(&persisted).Error; err != nil || !persisted.Valid() {
		return report, errors.New("persisted authorization migration marker is invalid")
	}
	if persisted.Checksum != marker.Checksum {
		return report, errors.New("persisted authorization migration marker does not match apply result")
	}
	report.Marker = &marker
	return report, nil
}

func writeReport(w io.Writer, report commandReport) error {
	encoder := json.NewEncoder(w)
	encoder.SetEscapeHTML(false)
	encoder.SetIndent("", "  ")
	return encoder.Encode(report)
}
