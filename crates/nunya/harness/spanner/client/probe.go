package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"

	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/structpb"
)

func probeScenario(ctx context.Context) error {
	conn, err := grpc.NewClient(os.Getenv("NUNYA_D2_NATIVE"), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer conn.Close()
	client := sppb.NewSpannerClient(conn)
	auth := metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+os.Getenv("NUNYA_D2_BEARER"))
	for _, mode := range []string{"regular", "multiplexed"} {
		if err := probeMode(auth, ctx, client, mode); err != nil {
			return err
		}
	}
	return nil
}

func probeMode(auth, direct context.Context, client sppb.SpannerClient, mode string) error {
	failed := false
	check := func(label string, ok bool) {
		verdict := "PASS"
		if !ok {
			verdict = "FAIL"
			failed = true
		}
		fmt.Printf("%s probe %s %s\n", verdict, mode, label)
	}
	mux := mode == "multiplexed"
	base := int64(700000)
	if mux {
		base = 710000
	}
	session, err := client.CreateSession(auth, &sppb.CreateSessionRequest{
		Database: os.Getenv("NUNYA_D2_DATABASE"), Session: &sppb.Session{Multiplexed: mux},
	})
	check("create-session", err == nil)
	if err != nil {
		return errors.New("probe create failed")
	}
	defer func() { _, _ = client.DeleteSession(auth, &sppb.DeleteSessionRequest{Name: session.GetName()}) }()
	rs, err := client.ExecuteSql(auth, &sppb.ExecuteSqlRequest{
		Session: session.GetName(), Seqno: 1,
		Sql:        "INSERT INTO Notes (Id, Body) VALUES (@id, 'probe')",
		Params:     &structpb.Struct{Fields: map[string]*structpb.Value{"id": structpb.NewStringValue(strconv.FormatInt(base+1, 10))}},
		ParamTypes: map[string]*sppb.Type{"id": {Code: sppb.TypeCode_INT64}},
		Transaction: &sppb.TransactionSelector{Selector: &sppb.TransactionSelector_Begin{Begin: &sppb.TransactionOptions{
			Mode: &sppb.TransactionOptions_ReadWrite_{ReadWrite: &sppb.TransactionOptions_ReadWrite{}},
		}}},
	})
	check("dml", err == nil && rs.GetStats().GetRowCountExact() == 1 && len(rs.GetMetadata().GetTransaction().GetId()) > 0)
	if err != nil {
		return errors.New("probe dml failed")
	}
	if mux {
		check("precommit-token", rs.GetPrecommitToken() != nil)
	}
	req := &sppb.CommitRequest{Session: session.GetName(),
		Transaction:    &sppb.CommitRequest_TransactionId{TransactionId: rs.GetMetadata().GetTransaction().GetId()},
		PrecommitToken: rs.GetPrecommitToken(),
	}
	cr, err := client.Commit(auth, req)
	retried := false
	if err == nil && cr.GetCommitTimestamp() == nil && cr.GetPrecommitToken() != nil {
		req.PrecommitToken = cr.GetPrecommitToken()
		retried = true
		cr, err = client.Commit(auth, req)
	}
	check("commit-timestamp", err == nil && cr.GetCommitTimestamp() != nil)
	count, countErr := commitCount(direct, base)
	check("upstream-count", countErr == nil && count == 1)
	fmt.Printf("INFO probe %s commit-code=%s retry=%t upstream-count=%d count-code=%s\n", mode, resultCode(err), retried, count, resultCode(countErr))
	if failed {
		return errors.New("probe assertions failed")
	}
	return nil
}
