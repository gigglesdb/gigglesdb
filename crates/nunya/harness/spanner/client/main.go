// Command client runs the native Spanner client compatibility scenarios.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"cloud.google.com/go/spanner"
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// rpcCounter counts attempts at the transport boundary, including library retries.
type rpcCounter struct {
	mu            sync.Mutex
	calls         map[string]int
	commitSession string
	commitTx      []byte
}

func (c *rpcCounter) count(method string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls[method]++
}

func (c *rpcCounter) unary(bearer string) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoke grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		c.count(method)
		if commit, ok := req.(*sppb.CommitRequest); ok {
			c.mu.Lock()
			c.commitSession = commit.GetSession()
			c.commitTx = append([]byte(nil), commit.GetTransactionId()...)
			c.mu.Unlock()
		}
		return invoke(metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+bearer), method, req, reply, cc, opts...)
	}
}

func (c *rpcCounter) stream(bearer string) grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		c.count(method)
		return streamer(metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+bearer), desc, cc, method, opts...)
	}
}

func newClient(ctx context.Context, calls *rpcCounter) (*spanner.Client, error) {
	bearer := os.Getenv("NUNYA_D2_BEARER")
	endpoint := os.Getenv("NUNYA_D2_NATIVE")
	if bearer == "" || endpoint == "" {
		return nil, errors.New("missing client configuration")
	}
	return spanner.NewClientWithConfig(ctx, os.Getenv("NUNYA_D2_DATABASE"), spanner.ClientConfig{
		NumChannels:          1,
		DisableNativeMetrics: true,
	}, option.WithEndpoint(endpoint), option.WithoutAuthentication(),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
		option.WithGRPCDialOption(grpc.WithChainUnaryInterceptor(calls.unary(bearer))),
		option.WithGRPCDialOption(grpc.WithChainStreamInterceptor(calls.stream(bearer))))
}

// readToken owns and closes one independent client object.
func readToken(ctx context.Context, calls *rpcCounter) (string, error) {
	client, err := newClient(ctx, calls)
	if err != nil {
		return "", err
	}
	defer client.Close()
	tx := client.Single()
	defer tx.Close()
	rows := tx.Query(ctx, spanner.Statement{SQL: "SELECT Email FROM Customers WHERE Id = 3"})
	defer rows.Stop()
	row, err := rows.Next()
	if err != nil {
		return "", err
	}
	var token string
	if err := row.Columns(&token); err != nil {
		return "", err
	}
	if _, err := rows.Next(); err != iterator.Done {
		return "", errors.New("expected exactly one row")
	}
	if token == "" || token == "carol@example.test" {
		return "", errors.New("blind value was not tokenized")
	}
	return token, nil
}

func resume(ctx context.Context, phase, reference string, calls *rpcCounter) error {
	first, err := readToken(ctx, calls)
	if err != nil {
		return err
	}
	if phase == "before" {
		fmt.Println("PASS resume blind-token")
		second, err := readToken(ctx, calls)
		if err != nil {
			return err
		}
		if first != second {
			return errors.New("token changed between client objects")
		}
		if err := os.WriteFile(reference, []byte(first), 0600); err != nil {
			return errors.New("cannot save token reference")
		}
		fmt.Println("PASS resume new-client-same-bearer")
		return nil
	}
	expected, err := os.ReadFile(reference)
	if err != nil {
		return errors.New("cannot read token reference")
	}
	if string(expected) != first {
		return errors.New("token changed after gateway restart")
	}
	fmt.Println("PASS resume restart-same-bearer")
	return nil
}

func main() {
	// Never let an inherited emulator setting bypass the Nunya endpoint.
	_ = os.Unsetenv("SPANNER_EMULATOR_HOST")
	_ = os.Setenv("SPANNER_DISABLE_BUILTIN_METRICS", "true")
	_ = os.Setenv("SPANNER_DISABLE_AFE_SERVER_TIMING", "true")
	_ = os.Setenv("SPANNER_DISABLE_DIRECT_ACCESS_GRPC_BUILTIN_METRICS", "true")
	_ = os.Setenv("GOOGLE_CLOUD_SPANNER_DISABLE_LOG_CLIENT_OPTIONS", "true")
	if len(os.Args) == 2 && os.Args[1] == "probe" {
		ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
		defer cancel()
		if err := probeScenario(ctx); err != nil {
			fmt.Printf("FAIL probe execution status-%s\n", status.Code(err))
			os.Exit(1)
		}
		return
	}
	if len(os.Args) == 3 && os.Args[1] == "commit" {
		ctx, cancel := context.WithTimeout(context.Background(), 55*time.Second)
		defer cancel()
		if err := commitScenario(ctx, os.Args[2]); err != nil {
			fmt.Printf("FAIL commit %s execution status-%s\n", os.Args[2], status.Code(err))
			os.Exit(1)
		}
		return
	}
	if len(os.Args) == 2 && (os.Args[1] == "upstream" || os.Args[1] == "stream") {
		ctx, cancel := context.WithTimeout(context.Background(), 110*time.Second)
		defer cancel()
		var err error
		if os.Args[1] == "upstream" {
			err = upstream(ctx)
		} else {
			err = streamScenario(ctx)
		}
		if err != nil {
			fmt.Printf("FAIL %s execution status-%s\n", os.Args[1], status.Code(err))
			os.Exit(1)
		}
		return
	}
	if len(os.Args) != 5 || os.Args[1] != "resume" || (os.Args[2] != "before" && os.Args[2] != "after") || os.Args[3] != "--reference" {
		fmt.Println("FAIL resume arguments expected-resume-before-or-after-and-reference")
		os.Exit(1)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	calls := &rpcCounter{calls: make(map[string]int)}
	if err := resume(ctx, os.Args[2], os.Args[4], calls); err != nil {
		// Upstream errors may contain data; expose only the status code.
		fmt.Printf("FAIL resume %s status-%s\n", os.Args[2], status.Code(err))
		os.Exit(1)
	}
}
