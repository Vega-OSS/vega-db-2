package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"

	"github.com/Vega-OSS/vega-db/gen/vegapb"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:50051", "gRPC server address")
	limit := flag.Int("limit", 0, "Limit for scan")
	flag.Parse()

	args := flag.Args()
	if len(args) == 0 {
		fmt.Fprintf(os.Stderr, "Usage: vegactl [--addr host:port] <put|get|del|health|scan|stats> [args...]\n")
		os.Exit(1)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn, err := grpc.NewClient(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to connect to %s: %v\n", *addr, err)
		os.Exit(1)
	}
	defer conn.Close()

	client := vegapb.NewVegaClient(conn)
	healthClient := healthpb.NewHealthClient(conn)

	cmd := args[0]
	switch cmd {
	case "health":
		resp, err := healthClient.Check(ctx, &healthpb.HealthCheckRequest{Service: "vega.v1.Vega"})
		if err != nil {
			fmt.Fprintf(os.Stderr, "health check error: %v\n", err)
			os.Exit(1)
		}
		if resp.GetStatus() != healthpb.HealthCheckResponse_SERVING {
			fmt.Fprintf(os.Stderr, "service not serving: %v\n", resp.GetStatus())
			os.Exit(1)
		}
		fmt.Println("SERVING")
		os.Exit(0)

	case "put":
		if len(args) < 3 {
			fmt.Fprintf(os.Stderr, "Usage: vegactl put <key> <value|@file>\n")
			os.Exit(1)
		}
		key := []byte(args[1])
		var val []byte
		valArg := args[2]
		if strings.HasPrefix(valArg, "@") {
			filePath := strings.TrimPrefix(valArg, "@")
			fVal, err := os.ReadFile(filePath)
			if err != nil {
				fmt.Fprintf(os.Stderr, "failed to read file %s: %v\n", filePath, err)
				os.Exit(1)
			}
			val = fVal
		} else {
			val = []byte(valArg)
		}

		_, err := client.Put(ctx, &vegapb.PutRequest{Key: key, Value: val})
		if err != nil {
			fmt.Fprintf(os.Stderr, "put error: %v\n", err)
			os.Exit(1)
		}
		os.Exit(0)

	case "get":
		if len(args) < 2 {
			fmt.Fprintf(os.Stderr, "Usage: vegactl get <key>\n")
			os.Exit(1)
		}
		key := []byte(args[1])
		resp, err := client.Get(ctx, &vegapb.GetRequest{Key: key})
		if err != nil {
			if status.Code(err) == codes.NotFound {
				fmt.Fprintf(os.Stderr, "key not found\n")
				os.Exit(2)
			}
			fmt.Fprintf(os.Stderr, "get error: %v\n", err)
			os.Exit(1)
		}
		os.Stdout.Write(resp.GetValue())
		fmt.Println()
		os.Exit(0)

	case "del":
		if len(args) < 2 {
			fmt.Fprintf(os.Stderr, "Usage: vegactl del <key>\n")
			os.Exit(1)
		}
		key := []byte(args[1])
		_, err := client.Delete(ctx, &vegapb.DeleteRequest{Key: key})
		if err != nil {
			fmt.Fprintf(os.Stderr, "del error: %v\n", err)
			os.Exit(1)
		}
		os.Exit(0)

	case "scan":
		if len(args) < 3 {
			fmt.Fprintf(os.Stderr, "Usage: vegactl scan <start> <end> [--limit N]\n")
			os.Exit(1)
		}
		start := []byte(args[1])
		end := []byte(args[2])
		stream, err := client.Scan(ctx, &vegapb.ScanRequest{Start: start, End: end, Limit: uint32(*limit)})
		if err != nil {
			fmt.Fprintf(os.Stderr, "scan error: %v\n", err)
			os.Exit(1)
		}
		for {
			kv, err := stream.Recv()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				fmt.Fprintf(os.Stderr, "scan stream error: %v\n", err)
				os.Exit(1)
			}
			fmt.Printf("%s\t%s\n", string(kv.GetKey()), string(kv.GetValue()))
		}
		os.Exit(0)

	case "stats":
		resp, err := client.Stats(ctx, &vegapb.StatsRequest{})
		if err != nil {
			fmt.Fprintf(os.Stderr, "stats error: %v\n", err)
			os.Exit(1)
		}
		for k, v := range resp.GetCounters() {
			fmt.Printf("%s: %s\n", k, strconv.FormatInt(v, 10))
		}
		os.Exit(0)

	default:
		fmt.Fprintf(os.Stderr, "unknown command: %s\n", cmd)
		os.Exit(1)
	}
}
