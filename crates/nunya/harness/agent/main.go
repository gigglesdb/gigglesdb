// Command agent-client exercises fresh native Spanner clients without printing data.
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
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const database = "projects/test-project/instances/test-instance/databases/gsql"

func client(ctx context.Context, upstream bool) (*spanner.Client, error) {
	endpoint := os.Getenv("AGENT_ENDPOINT")
	options := []option.ClientOption{option.WithoutAuthentication(), option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials()))}
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

func single(ctx context.Context, sql string, params map[string]any) ([]*spanner.Row, error) {
	c, err := client(ctx, false)
	if err != nil {
		return nil, err
	}
	defer c.Close()
	tx := c.Single()
	defer tx.Close()
	rows := tx.Query(ctx, spanner.Statement{SQL: sql, Params: params})
	defer rows.Stop()
	var result []*spanner.Row
	for {
		row, err := rows.Next()
		if err == iterator.Done {
			return result, nil
		}
		if err != nil {
			return nil, err
		}
		result = append(result, row)
	}
}

func readToken(ctx context.Context) (string, error) {
	rows, err := single(ctx, "SELECT Email FROM Customers WHERE Id = 3", nil)
	if err != nil {
		return "", err
	}
	if len(rows) != 1 {
		return "", errors.New("row count")
	}
	var token string
	if err = rows[0].Columns(&token); err != nil {
		return "", err
	}
	if !strings.HasPrefix(token, "tok_email_") {
		return "", errors.New("token shape")
	}
	return token, nil
}

func cycle(ctx context.Context) error {
	token, err := readToken(ctx)
	if err != nil {
		return err
	}
	path := os.Getenv("AGENT_REFERENCE")
	reference, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		err = os.WriteFile(path, []byte(token), 0600)
	} else if err == nil && string(reference) != token {
		return errors.New("token changed")
	}
	if err != nil {
		return err
	}
	rows, err := single(ctx, "SELECT Id FROM Customers WHERE Email = @token", map[string]any{"token": token})
	if err != nil {
		return err
	}
	if len(rows) != 1 {
		return errors.New("bound row count")
	}
	var id int64
	if err = rows[0].Columns(&id); err != nil {
		return err
	}
	if id != 3 {
		return errors.New("bound row")
	}
	again, err := readToken(ctx)
	if err != nil {
		return err
	}
	if again != token {
		return errors.New("token changed")
	}
	return nil
}

func quota(ctx context.Context) error {
	successes := 0
	code := "none"
	ok := false
	defer func() {
		result := "FAIL"
		if ok {
			result = "OK"
		}
		fmt.Printf("%s successes=%d code=%s\n", result, successes, code)
	}()
	for i := 0; i < 21; i++ {
		rows, err := single(ctx, "SELECT nunya_tokenize('email', @v)", map[string]any{"v": "quota@agent.test"})
		if err != nil {
			if measured := regexp.MustCompile(`\bNY[0-9]{3}\b`).FindString(status.Convert(err).Message()); measured != "" {
				code = measured
			}
			if code == "NY007" && successes == 20 {
				ok = true
				return nil
			}
			return errors.New("quota refusal")
		}
		if len(rows) != 1 {
			return errors.New("quota rows")
		}
		var token string
		if err = rows[0].Columns(&token); err != nil {
			return err
		}
		if !strings.HasPrefix(token, "tok_email_") {
			return errors.New("quota token")
		}
		successes++
	}
	return errors.New("quota not enforced")
}

func seed(ctx context.Context) error {
	c, err := client(ctx, true)
	if err != nil {
		return err
	}
	defer c.Close()
	for first := int64(1000); first < 21000; first += 100 {
		mutations := make([]*spanner.Mutation, 0, 100)
		for id := first; id < first+100; id++ {
			mutations = append(mutations, spanner.InsertOrUpdate("Customers", []string{"Id", "Email", "Country", "Phone", "FailedPayments"}, []any{id, fmt.Sprintf("user%d@agent.test", id), strings.Repeat("C", 16384), nil, int64(0)}))
		}
		if _, err = c.Apply(ctx, mutations); err != nil {
			return err
		}
	}
	return nil
}

func stream(ctx context.Context) error {
	c, err := client(ctx, false)
	if err != nil {
		return err
	}
	defer c.Close()
	tx := c.Single()
	defer tx.Close()
	rows := tx.Query(ctx, spanner.Statement{SQL: "SELECT Id, Country FROM Customers WHERE Id >= 1000"})
	defer rows.Stop()
	count := 0
	for {
		_, err = rows.Next()
		if err != nil {
			if count >= 200 && count < 20000 && strings.Contains(status.Convert(err).Message(), "NY011") {
				return nil
			}
			return errors.New("stream refusal")
		}
		count++
		if count <= 200 {
			time.Sleep(2 * time.Millisecond)
		}
		if count == 200 {
			cmd := exec.CommandContext(ctx, os.Getenv("AGENT_BINARY"), "session", "revoke", "--server", os.Getenv("AGENT_SERVER"), "--principal", "agent", "--principal-secret-file", os.Getenv("AGENT_SECRET"), "--context", "agent-sp")
			if err = cmd.Run(); err != nil {
				return errors.New("revoke failed")
			}
		}
	}
}

func run(ctx context.Context, mode string) error {
	switch mode {
	case "seed":
		return seed(ctx)
	case "cycle":
		return cycle(ctx)
	case "quota":
		return quota(ctx)
	case "stream":
		return stream(ctx)
	case "refused":
		_, err := readToken(ctx)
		if err != nil && strings.Contains(status.Convert(err).Message(), "NY003") {
			return nil
		}
		return errors.New("new connection accepted")
	default:
		return errors.New("unknown mode")
	}
}

func main() {
	_ = os.Unsetenv("SPANNER_EMULATOR_HOST")
	for _, key := range []string{"SPANNER_DISABLE_BUILTIN_METRICS", "SPANNER_DISABLE_AFE_SERVER_TIMING", "SPANNER_DISABLE_DIRECT_ACCESS_GRPC_BUILTIN_METRICS", "GOOGLE_CLOUD_SPANNER_DISABLE_LOG_CLIENT_OPTIONS"} {
		_ = os.Setenv(key, "true")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 210*time.Second)
	defer cancel()
	if len(os.Args) != 2 {
		fmt.Println("ERROR arguments")
		os.Exit(1)
	}
	if os.Args[1] == "quota" {
		_ = quota(ctx)
		return
	}
	if err := run(ctx, os.Args[1]); err != nil {
		fmt.Println("ERROR native-check")
		os.Exit(1)
	}
	fmt.Println("OK")
}
