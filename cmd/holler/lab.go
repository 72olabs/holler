package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/72olabs/holler/internal/lab"
)

func runLab(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 {
		fmt.Fprintln(stderr, "usage: holler lab list | run [--scenario NAME|--file PATH] [options]")
		return flag.ErrHelp
	}
	switch args[0] {
	case "list":
		if len(args) != 1 {
			return fmt.Errorf("holler lab list does not accept arguments")
		}
		names, err := lab.BuiltInScenarioNames()
		if err != nil {
			return err
		}
		return writeJSON(stdout, names)
	case "run":
		flags := commandFlags("lab run", stderr)
		scenarioName := flags.String("scenario", "direct-roundtrip", "built-in scenario name")
		scenarioFile := flags.String("file", "", "external YAML scenario")
		runAll := flags.Bool("all", false, "run every built-in deterministic scenario")
		outputDir := flags.String("output", "", "evidence output directory; defaults under .runs/lab")
		hollerdBinary := flags.String("hollerd", "", "hollerd binary; defaults next to this holler executable")
		keepSandbox := flags.Bool("keep-sandbox", false, "retain isolated runtime state after the run")
		includeDatabase := flags.Bool("include-database", false, "retain the stopped SQLite database; message bodies are otherwise omitted")
		timeout := flags.Duration("timeout", 30*time.Second, "wall-time budget")
		if err := flags.Parse(args[1:]); err != nil {
			return err
		}
		if flags.NArg() != 0 {
			return fmt.Errorf("unexpected lab arguments: %s", strings.Join(flags.Args(), " "))
		}
		if strings.TrimSpace(*hollerdBinary) == "" {
			executable, err := os.Executable()
			if err != nil {
				return err
			}
			*hollerdBinary = filepath.Join(filepath.Dir(executable), "hollerd")
		}
		if *runAll {
			if strings.TrimSpace(*scenarioFile) != "" {
				return fmt.Errorf("--all and --file cannot be used together")
			}
			names, err := lab.BuiltInScenarioNames()
			if err != nil {
				return err
			}
			reports := make([]lab.Report, 0, len(names))
			var failures []error
			for _, name := range names {
				scenario, _, err := lab.LoadScenario(name, "")
				if err != nil {
					return err
				}
				scenarioOutput := ""
				if strings.TrimSpace(*outputDir) != "" {
					scenarioOutput = filepath.Join(*outputDir, name)
				}
				report, runErr := lab.Run(ctx, lab.Config{
					Scenario: scenario, HollerdBinary: *hollerdBinary, OutputDir: scenarioOutput,
					KeepSandbox: *keepSandbox, IncludeDatabase: *includeDatabase, Timeout: *timeout,
				})
				reports = append(reports, report)
				if runErr != nil {
					failures = append(failures, fmt.Errorf("%s: %w", name, runErr))
				}
			}
			if err := writeJSON(stdout, reports); err != nil {
				return err
			}
			if len(failures) > 0 {
				return fmt.Errorf("%d lab scenarios failed: %v", len(failures), failures)
			}
			return nil
		}
		scenario, _, err := lab.LoadScenario(*scenarioName, *scenarioFile)
		if err != nil {
			return err
		}
		report, runErr := lab.Run(ctx, lab.Config{
			Scenario: scenario, HollerdBinary: *hollerdBinary, OutputDir: *outputDir,
			KeepSandbox: *keepSandbox, IncludeDatabase: *includeDatabase, Timeout: *timeout,
		})
		if err := writeJSON(stdout, report); err != nil {
			return err
		}
		return runErr
	default:
		return fmt.Errorf("unknown lab command %q", args[0])
	}
}
