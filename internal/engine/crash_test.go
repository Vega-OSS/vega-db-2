package engine_test

import (
	"bufio"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"syscall"
	"testing"
	"time"

	"vegadb/internal/engine"
)

// TestHelperCrashWorker is executed in a subprocess during crash tests.
func TestHelperCrashWorker(t *testing.T) {
	if os.Getenv("VEGA_CRASH_HELPER") != "1" {
		return
	}

	dir := os.Getenv("VEGA_TEST_DIR")
	if dir == "" {
		t.Fatal("VEGA_TEST_DIR not set")
	}

	opts := engine.DefaultOptions()
	opts.Dir = dir
	opts.Sync = engine.SyncAlways
	opts.MemtableSize = 8 * 1024     // 8 KiB tiny memtable
	opts.VlogSegmentSize = 16 * 1024 // 16 KiB tiny segments

	db, err := engine.Open(opts)
	if err != nil {
		fmt.Fprintf(os.Stderr, "worker open err: %v\n", err)
		os.Exit(2)
	}
	defer db.Close()

	for i := range 10000 {
		k := fmt.Sprintf("ck%05d", i)
		v := fmt.Sprintf("v%05d", i)
		if err := db.Put([]byte(k), []byte(v)); err != nil {
			fmt.Fprintf(os.Stderr, "worker put err: %v\n", err)
			os.Exit(2)
		}
		// Acknowledged write: output to stdout
		fmt.Println(k)
	}
}

func runCrashIteration(t *testing.T, dir string, failpointName string, killAfterMs int) {
	t.Helper()

	cmd := exec.Command(os.Args[0], "-test.run=TestHelperCrashWorker")
	cmd.Env = append(os.Environ(),
		"VEGA_CRASH_HELPER=1",
		"VEGA_TEST_DIR="+dir,
	)
	if failpointName != "" {
		cmd.Env = append(cmd.Env, "VEGA_FAILPOINT="+failpointName)
	}

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatalf("stdout pipe: %v", err)
	}

	if err := cmd.Start(); err != nil {
		t.Fatalf("start helper: %v", err)
	}

	var ackedKeys []string
	scanner := bufio.NewScanner(stdout)

	doneScan := make(chan struct{})
	go func() {
		for scanner.Scan() {
			text := scanner.Text()
			if text != "" {
				ackedKeys = append(ackedKeys, text)
			}
		}
		close(doneScan)
	}()

	if failpointName == "" {
		// Random SIGKILL delay
		time.Sleep(time.Duration(killAfterMs) * time.Millisecond)
		_ = cmd.Process.Signal(syscall.SIGKILL)
	}

	_ = cmd.Wait()
	<-doneScan

	if len(ackedKeys) == 0 {
		return
	}

	// Reopen DB and verify every acknowledged key survived crash
	opts := engine.DefaultOptions()
	opts.Dir = dir
	opts.Sync = engine.SyncAlways
	opts.MemtableSize = 8 * 1024
	opts.VlogSegmentSize = 16 * 1024

	db, err := engine.Open(opts)
	if err != nil {
		t.Fatalf("reopen after crash: %v", err)
	}
	defer db.Close()

	for _, k := range ackedKeys {
		var idx int
		fmt.Sscanf(k, "ck%05d", &idx)
		expectedVal := fmt.Sprintf("v%05d", idx)

		val, err := db.Get([]byte(k), nil)
		if err != nil {
			t.Fatalf("key %s was acknowledged before crash but missing after recovery: %v", k, err)
		}
		if string(val) != expectedVal {
			t.Fatalf("key %s recovered data mismatch: got %s, want %s", k, string(val), expectedVal)
		}
	}
}

func TestEngineCrashRecovery(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping crash tests in short mode")
	}

	rng := rand.New(rand.NewSource(777))
	const iterations = 50

	for i := range iterations {
		dir := t.TempDir()
		killDelay := 20 + rng.Intn(80)
		runCrashIteration(t, dir, "", killDelay)
		if (i+1)%10 == 0 {
			t.Logf("completed crash iteration %d/%d", i+1, iterations)
		}
	}
}

func TestEngineFailpoints(t *testing.T) {
	failpoints := []string{
		"flush_after_sst_before_manifest",
		"flush_after_manifest",
		"compaction_after_output_before_manifest",
		"vlog_mid_batch",
	}

	for _, fp := range failpoints {
		t.Run(fp, func(t *testing.T) {
			dir := t.TempDir()
			runCrashIteration(t, dir, fp, 0)
		})
	}
}
