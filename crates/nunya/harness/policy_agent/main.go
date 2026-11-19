// Persistent native Spanner client; stdout is a private parent/child protocol.
package main

import (
	"bufio"
	"cloud.google.com/go/spanner"
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"io"
	"os"
	"regexp"
	"strings"
	"time"
)

const database = "projects/test-project/instances/test-instance/databases/gsql"

func client(ctx context.Context, upstream bool) (*spanner.Client, error) {
	endpoint := os.Getenv("AGENT_ENDPOINT")
	// Keep the bounded fixture in flight while the parent applies policy.
	// Dynamic HTTP/2 window growth can otherwise buffer the entire result.
	options := []option.ClientOption{option.WithoutAuthentication(),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
		option.WithGRPCDialOption(grpc.WithStaticStreamWindowSize(64 * 1024)),
		option.WithGRPCDialOption(grpc.WithStaticConnWindowSize(64 * 1024))}
	if upstream {
		endpoint = os.Getenv("AGENT_EMULATOR")
	} else {
		bearer := os.Getenv("AGENT_BEARER")
		if bearer == "" {
			return nil, errors.New("missing bearer")
		}
		options = append(options,
			option.WithGRPCDialOption(grpc.WithChainUnaryInterceptor(func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoke grpc.UnaryInvoker, opts ...grpc.CallOption) error {
				return invoke(metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+bearer), method, req, reply, cc, opts...)
			})),
			option.WithGRPCDialOption(grpc.WithChainStreamInterceptor(func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
				return streamer(metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+bearer), desc, cc, method, opts...)
			})))
	}
	options = append(options, option.WithEndpoint(endpoint))
	return spanner.NewClientWithConfig(ctx, database, spanner.ClientConfig{NumChannels: 1, DisableNativeMetrics: true}, options...)
}

func code(err error) string {
	v := regexp.MustCompile(`NY[0-9]{3}`).FindString(err.Error())
	if v == "" {
		return "native-error"
	}
	return v
}
func cell(row *spanner.Row, i int) (string, error) {
	var generic spanner.GenericColumnValue
	if err := row.Column(i, &generic); err != nil {
		return "", err
	}
	var value string
	switch generic.Type.Code.String() {
	case "INT64":
		var v spanner.NullInt64
		if err := generic.Decode(&v); err != nil {
			return "", err
		}
		if !v.Valid {
			return "~", nil
		}
		value = fmt.Sprint(v.Int64)
	case "JSON":
		var v spanner.NullJSON
		if err := generic.Decode(&v); err != nil {
			return "", err
		}
		if !v.Valid {
			return "~", nil
		}
		b, err := json.Marshal(v.Value)
		if err != nil {
			return "", err
		}
		value = string(b)
	case "STRING":
		var v spanner.NullString
		if err := generic.Decode(&v); err != nil {
			return "", err
		}
		if !v.Valid {
			return "~", nil
		}
		value = v.StringVal
	default:
		return "", errors.New("unsupported-result")
	}
	return base64.StdEncoding.EncodeToString([]byte(value)), nil
}
func main() {
	_ = os.Unsetenv("SPANNER_EMULATOR_HOST")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	c, err := client(ctx, false)
	if err != nil {
		fmt.Println("CODE setup")
		return
	}
	defer c.Close()
	var rawConn *grpc.ClientConn
	var raw sppb.SpannerClient
	var session string
	var stream sppb.Spanner_ExecuteStreamingSqlClient
	streamCtx := metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+os.Getenv("AGENT_BEARER"))
	closeStream := func() {
		if rawConn != nil {
			if session != "" {
				_, _ = raw.DeleteSession(streamCtx, &sppb.DeleteSessionRequest{Name: session})
			}
			_ = rawConn.Close()
			rawConn = nil
		}
	}
	defer closeStream()
	fmt.Println("READY")
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		line := scanner.Text()
		switch line {
		case "quit":
			if stream != nil {
				closeStream()
			}
			fmt.Println("BYE")
			return
		case "reconnect":
			c.Close()
			c, err = client(ctx, false)
			if err != nil {
				fmt.Println("CODE setup")
			} else {
				fmt.Println("OK")
			}
			continue
		case "stream":
			// The high-level iterator buffers until a resume token, so receive native
			// parts directly to pause while this bounded result is still in flight.
			rawConn, err = grpc.NewClient(os.Getenv("AGENT_ENDPOINT"),
				grpc.WithTransportCredentials(insecure.NewCredentials()),
				grpc.WithStaticStreamWindowSize(64*1024), grpc.WithStaticConnWindowSize(64*1024))
			if err != nil {
				fmt.Println("CODE setup")
				continue
			}
			raw = sppb.NewSpannerClient(rawConn)
			var created *sppb.Session
			created, err = raw.CreateSession(streamCtx, &sppb.CreateSessionRequest{Database: database})
			if err == nil {
				session = created.Name
				stream, err = raw.ExecuteStreamingSql(streamCtx, &sppb.ExecuteSqlRequest{
					Session: session, Sql: "SELECT Id, Body FROM Notes ORDER BY Id",
					Transaction: &sppb.TransactionSelector{Selector: &sppb.TransactionSelector_SingleUse{
						SingleUse: &sppb.TransactionOptions{Mode: &sppb.TransactionOptions_ReadOnly_{
							ReadOnly: &sppb.TransactionOptions_ReadOnly{TimestampBound: &sppb.TransactionOptions_ReadOnly_Strong{Strong: true}}}}}}})
			}
			if err == nil {
				_, err = stream.Recv()
			}
			if err != nil {
				fmt.Println("CODE " + code(err))
			} else {
				fmt.Println("READY")
			}
			continue
		case "drain":
			for {
				_, err = stream.Recv()
				if err != nil {
					break
				}
			}
			closeStream()
			stream = nil
			if err == io.EOF {
				fmt.Println("END")
			} else {
				fmt.Println("CODE " + code(err))
			}
			continue
		}
		sql, e := base64.StdEncoding.DecodeString(line)
		if e != nil {
			fmt.Println("CODE protocol")
			continue
		}
		t := c.Single()
		rs := t.Query(ctx, spanner.Statement{SQL: string(sql)})
		for {
			r, e := rs.Next()
			if e == iterator.Done {
				fmt.Println("OK")
				break
			}
			if e != nil {
				fmt.Println("CODE " + code(e))
				break
			}
			cells := make([]string, r.Size())
			good := true
			for i := range cells {
				cells[i], e = cell(r, i)
				if e != nil {
					good = false
					break
				}
			}
			if !good {
				fmt.Println("CODE decode")
				break
			}
			fmt.Println("ROW " + strings.Join(cells, " "))
		}
		rs.Stop()
		t.Close()
	}
}
