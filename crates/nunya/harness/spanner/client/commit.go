package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"cloud.google.com/go/spanner"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

func (c *rpcCounter) methodCount(method string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls["/google.spanner.v1.Spanner/"+method]
}

func commitCount(ctx context.Context, base int64) (int64, error) {
	callCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	endpoint := os.Getenv("NUNYA_D2_EMULATOR_GRPC")
	if endpoint == "" {
		return 0, errors.New("missing emulator endpoint")
	}
	client, err := spanner.NewClientWithConfig(callCtx, os.Getenv("NUNYA_D2_DATABASE"), spanner.ClientConfig{
		NumChannels: 1, DisableNativeMetrics: true,
	}, option.WithEndpoint(strings.TrimPrefix(endpoint, "http://")), option.WithoutAuthentication(),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())))
	if err != nil {
		return 0, err
	}
	defer client.Close()
	tx := client.Single()
	defer tx.Close()
	rows := tx.Query(callCtx, spanner.Statement{SQL: "SELECT COUNT(*) FROM Notes WHERE Id > @base AND Id <= @end", Params: map[string]any{"base": base, "end": base + 10}})
	defer rows.Stop()
	row, err := rows.Next()
	if err != nil {
		return 0, err
	}
	var count int64
	if err := row.Columns(&count); err != nil {
		return 0, err
	}
	if _, err := rows.Next(); err != iterator.Done {
		return 0, errors.New("expected one count row")
	}
	return count, nil
}

func commitAction(ctx context.Context, name string) error {
	switch name {
	case "control", "stream-dml":
		return nil
	case "revoked", "restart":
		variable := "NUNYA_D2_REVOKE"
		if name == "restart" {
			variable = "NUNYA_D2_RESTART"
		}
		hook := os.Getenv(variable)
		if hook == "" {
			return errors.New("missing commit hook")
		}
		hookCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
		defer cancel()
		if err := exec.CommandContext(hookCtx, "bash", hook).Run(); err != nil {
			return errors.New("commit hook failed")
		}
		return nil
	case "expired":
		expiry, err := strconv.ParseInt(os.Getenv("NUNYA_D2_EXPIRES_MS"), 10, 64)
		if err != nil || expiry <= 0 {
			return errors.New("missing session expiry")
		}
		delay := time.Until(time.UnixMilli(expiry).Add(time.Second))
		if delay <= 0 {
			return nil
		}
		timer := time.NewTimer(delay)
		defer timer.Stop()
		select {
		case <-timer.C:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	default:
		return errors.New("unknown commit action")
	}
}

func commitScenario(ctx context.Context, name string) error {
	var base int64
	switch name {
	case "control":
		base = 600000
	case "stream-dml":
		base = 640000
	case "revoked":
		base = 610000
	case "expired":
		base = 620000
	case "restart":
		base = 630000
	default:
		return errors.New("unknown commit case")
	}
	calls := &rpcCounter{calls: make(map[string]int)}
	client, err := newClient(ctx, calls)
	if err != nil {
		return err
	}
	defer client.Close()
	var runs atomic.Int64
	var dmlErr, actionErr error
	var rowCount int64
	_, ended := client.ReadWriteTransaction(ctx, func(bodyCtx context.Context, tx *spanner.ReadWriteTransaction) error {
		n := runs.Add(1)
		callCtx, cancel := context.WithTimeout(bodyCtx, 10*time.Second)
		statement := spanner.Statement{
			SQL:    fmt.Sprintf("INSERT INTO Notes (Id, Body) VALUES (@id, 'd2-%s')", name),
			Params: map[string]any{"id": base + n},
		}
		if name == "stream-dml" {
			rows := tx.Query(callCtx, statement)
			for {
				_, nextErr := rows.Next()
				if nextErr == iterator.Done {
					break
				}
				if nextErr != nil {
					dmlErr = nextErr
					break
				}
			}
			rowCount = rows.RowCount
			rows.Stop()
		} else {
			rowCount, dmlErr = tx.Update(callCtx, statement)
		}
		cancel()
		if dmlErr != nil {
			return dmlErr
		}
		actionErr = commitAction(bodyCtx, name)
		return actionErr
	})
	count, countErr := commitCount(ctx, base)
	commits := calls.methodCount("Commit")
	fmt.Printf("INFO commit %s table=Notes code=%s ny=%s runs=%d ExecuteSql=%d ExecuteStreamingSql=%d BeginTransaction=%d Commit=%d Rollback=%d upstream-count=%d count-code=%s dml-code=%s action-code=%s\n",
		name, resultCode(ended), nyCode(ended), runs.Load(), calls.methodCount("ExecuteSql"), calls.methodCount("ExecuteStreamingSql"), calls.methodCount("BeginTransaction"), commits, calls.methodCount("Rollback"), count, resultCode(countErr), resultCode(dmlErr), resultCode(actionErr))
	failed := false
	check := func(label string, ok bool) {
		verdict := "PASS"
		if !ok {
			verdict = "FAIL"
			failed = true
		}
		fmt.Printf("%s commit %s %s\n", verdict, name, label)
	}
	okStatus, okCount := false, false
	switch name {
	case "control", "stream-dml":
		okStatus, okCount = ended == nil, count == 1
	case "revoked", "expired":
		okStatus = status.Code(ended) == codes.PermissionDenied && nyCode(ended) == "NY003"
		okCount = count == 0
	case "restart":
		switch {
		case ended == nil:
			okStatus, okCount = true, count == 1
		case nyCode(ended) == "NY011" || nyCode(ended) == "NY012":
			okStatus, okCount = true, count >= 0 && count <= 1
		case status.Code(ended) == codes.PermissionDenied && nyCode(ended) == "NY003":
			okStatus, okCount = true, count == 0
		}
	}
	check("status", okStatus && dmlErr == nil && actionErr == nil)
	check("ran-once", runs.Load() == 1)
	check("commit-attempts", commits == 1)
	check("upstream-count", countErr == nil && okCount)
	if name == "control" || name == "stream-dml" {
		calls.mu.Lock()
		mux, regular := 0, 0
		for _, multiplexed := range calls.sessions {
			if multiplexed {
				mux++
			} else {
				regular++
			}
		}
		muxOK := len(calls.commits) == 1
		if muxOK {
			commit := calls.commits[0]
			multiplexed, known := calls.sessions[commit.session]
			wantMux := os.Getenv("NUNYA_D2_MUX_RW") != "off"
			muxOK = known && multiplexed == wantMux && commit.precommit == wantMux
			fmt.Printf("INFO commit %s mux-session=%t precommit-token=%t\n", name, multiplexed, commit.precommit)
		}
		calls.mu.Unlock()
		fmt.Printf("INFO mux created-multiplexed=%d created-regular=%d\n", mux, regular)
		check("mux", muxOK)
	}
	if name == "stream-dml" {
		fmt.Printf("INFO commit stream-dml RowCount=%d\n", rowCount)
		check("streaming-dml", calls.methodCount("ExecuteStreamingSql") >= 1 && calls.methodCount("ExecuteSql") == 0 && rowCount == 1)
	}
	if failed {
		return errors.New("commit assertions failed")
	}
	return nil
}
