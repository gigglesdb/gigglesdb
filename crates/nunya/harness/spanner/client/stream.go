package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"time"

	"cloud.google.com/go/spanner"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

const streamingSQL = "/google.spanner.v1.Spanner/ExecuteStreamingSql"

func (c *rpcCounter) snapshot() (stream, other int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for method, count := range c.calls {
		if method == streamingSQL {
			stream += count
		} else {
			other += count
		}
	}
	return
}

// upstream seeds blind values directly, outside the gateway policy boundary.
func upstream(ctx context.Context) error {
	endpoint := os.Getenv("NUNYA_D2_EMULATOR_GRPC")
	if endpoint == "" {
		return errors.New("missing emulator endpoint")
	}
	client, err := spanner.NewClientWithConfig(ctx, os.Getenv("NUNYA_D2_DATABASE"), spanner.ClientConfig{
		NumChannels: 1, DisableNativeMetrics: true,
	}, option.WithEndpoint(strings.TrimPrefix(endpoint, "http://")), option.WithoutAuthentication(),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())))
	if err != nil {
		return err
	}
	defer client.Close()
	for first := int64(1000); first < 21000; first += 100 {
		mutations := make([]*spanner.Mutation, 0, 100)
		for id := first; id < first+100; id++ {
			mutations = append(mutations, spanner.InsertOrUpdate("Customers",
				[]string{"Id", "Email", "Country", "Phone", "FailedPayments"},
				[]any{id, fmt.Sprintf("user%d@example.test", id), strings.Repeat("C", 16384), nil, int64(0)}))
		}
		callCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		_, err := client.Apply(callCtx, mutations)
		cancel()
		if err != nil {
			return err
		}
	}
	fmt.Println("INFO upstream seeded=20000")
	return nil
}

var nyCodePattern = regexp.MustCompile(`NY0[0-9][0-9]`)

func nyCode(err error) string {
	if err == iterator.Done {
		return ""
	}
	return nyCodePattern.FindString(status.Convert(err).Message())
}

func resultCode(err error) string {
	if err == iterator.Done {
		return "done"
	}
	return status.Code(err).String()
}

func revoked(err error) bool {
	return status.Code(err) == codes.PermissionDenied && nyCode(err) == "NY011"
}

func streamScenario(ctx context.Context) error {
	fmt.Println("INFO stream note=emulator-sends-no-resume-tokens")
	hook := os.Getenv("NUNYA_D2_REVOKE")
	if hook == "" {
		return errors.New("missing revoke hook")
	}
	calls := &rpcCounter{calls: make(map[string]int)}
	client, err := newClient(ctx, calls)
	if err != nil {
		return err
	}
	defer client.Close()
	tx := client.Single()
	defer tx.Close()
	queryCtx, cancelQuery := context.WithTimeout(ctx, 75*time.Second)
	defer cancelQuery()
	before, _ := calls.snapshot()
	rows := tx.Query(queryCtx, spanner.Statement{SQL: "SELECT Id, Email, Country FROM Customers WHERE Id >= 1000"})
	defer rows.Stop()
	total, atRevoke := 0, 0
	for {
		_, err = rows.Next()
		if err != nil {
			break
		}
		total++
		if total <= 200 {
			time.Sleep(2 * time.Millisecond)
		}
		if total == 200 {
			hookCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
			// CLI output is private; never print a secret or upstream error body.
			hookErr := exec.CommandContext(hookCtx, "bash", hook).Run()
			cancel()
			if hookErr != nil {
				return errors.New("revoke hook failed")
			}
			atRevoke = total
		}
	}
	ended := err
	after, _ := calls.snapshot()
	failed := false
	check := func(name string, ok bool, detail string) {
		verdict := "PASS"
		if !ok {
			verdict = "FAIL"
			failed = true
		}
		fmt.Printf("%s stream %s%s\n", verdict, name, detail)
	}
	check("status", revoked(ended), fmt.Sprintf(" code=%s ny=%s", resultCode(ended), nyCode(ended)))
	check("not-done", ended != iterator.Done && revoked(ended) && atRevoke == 200 && total < 20000, "")
	noRows := true
	for i := 0; i < 3; i++ {
		row, nextErr := rows.Next()
		noRows = noRows && row == nil && nextErr != nil && nextErr != iterator.Done
	}
	check("no-rows-after-error", noRows, "")
	check("no-silent-retry", after-before == 1, fmt.Sprintf(" before=%d after=%d", before, after))
	afterRevoke := total - atRevoke
	fmt.Printf("INFO stream at-revoke=%d after-revoke=%d total=%d\n", atRevoke, afterRevoke, total)
	if afterRevoke >= 20000-200 {
		check("buffer-bound", false, "")
	}

	refusedCalls := &rpcCounter{calls: make(map[string]int)}
	refusedCtx, cancelRefused := context.WithTimeout(ctx, 10*time.Second)
	defer cancelRefused()
	started := time.Now()
	_, refusedErr := readToken(refusedCtx, refusedCalls)
	elapsed := time.Since(started)
	streams, other := refusedCalls.snapshot()
	check("refused-after", status.Code(refusedErr) == codes.PermissionDenied && nyCode(refusedErr) == "NY003" && elapsed < 10*time.Second && streams <= 1, fmt.Sprintf(" code=%s ny=%s execute-streaming-sql=%d other=%d",
		resultCode(refusedErr), nyCode(refusedErr), streams, other))
	if failed {
		return errors.New("stream assertions failed")
	}
	return nil
}
