package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"cloud.google.com/go/spanner"
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/kelseyhightower/envconfig"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Config struct {
	GCPProjectID    string `envconfig:"GCP_PROJECT_ID"`
	SpannerInstance string `envconfig:"SPANNER_INSTANCE"`
	SpannerDatabase string `envconfig:"SPANNER_DATABASE"`
	CPUPriority     string `envconfig:"CPU_PRIORITY"`
}

func dbpath(projectID, instanceName, databaseName string) string {
	return fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectID, instanceName, databaseName)
}

func newSpannerClient(ctx context.Context, projectID, instanceName, databaseName string, config spanner.ClientConfig) (*spanner.Client, error) {
	return spanner.NewClientWithConfig(ctx, dbpath(projectID, instanceName, databaseName), config)
}

func checkPriority(priority string) error {
	p := strings.ToLower(priority)

	if p == "high" || p == "medium" || p == "low" {
		return nil
	}

	return fmt.Errorf("invalid priority: %s,　You can set the CPU priority as low, medium, or high", priority)
}

func priorityStringToRequestOption(priority string) sppb.RequestOptions_Priority {
	switch strings.ToLower(priority) {
	case "low":
		return sppb.RequestOptions_PRIORITY_LOW
	case "medium":
		return sppb.RequestOptions_PRIORITY_MEDIUM
	case "high":
		return sppb.RequestOptions_PRIORITY_HIGH
	default:
		return sppb.RequestOptions_PRIORITY_UNSPECIFIED
	}
}

var version string

func main() {
	var con uint64
	var duration time.Duration
	var query string
	var project string
	var instance string
	var db string
	var priorityStr string
	var showVersion bool
	flag.Uint64Var(&con, "c", 1, "Concurrency")
	flag.DurationVar(&duration, "d", 60*time.Second, "Duration")
	flag.StringVar(&query, "query", "", "SQL")
	flag.StringVar(&project, "project", "", "GCP Project")
	flag.StringVar(&instance, "instance", "", "Cloud Spanner Instance")
	flag.StringVar(&db, "database", "", "Cloud Spanner Database")
	flag.StringVar(&priorityStr, "priority", "high", "CPU priority: low, medium, high")
	flag.BoolVar(&showVersion, "version", false, "Show version")
	flag.Parse()

	if showVersion {
		fmt.Println(version)
		os.Exit(0)
	}

	if query == "" {
		log.Fatal("-query is required")
	}

	var c Config
	err := envconfig.Process("spanner-loader", &c)
	if err != nil {
		log.Fatal(err)
	}

	if err := checkPriority(priorityStr); err != nil {
		log.Fatal(err)
	}

	if project != "" {
		c.GCPProjectID = project
	}

	if instance != "" {
		c.SpannerInstance = instance
	}

	if db != "" {
		c.SpannerDatabase = db
	}

	if priorityStr != "" {
		c.CPUPriority = priorityStr
	}

	priority := priorityStringToRequestOption(c.CPUPriority)

	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	option := spanner.ClientConfig{
		SessionPoolConfig: spanner.SessionPoolConfig{
			MinOpened: con,
			MaxOpened: con,
			MaxIdle:   con,
			MaxBurst:  con,
		},
	}

	client, err := newSpannerClient(ctx, c.GCPProjectID, c.SpannerInstance, c.SpannerDatabase, option)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	var eg errgroup.Group

	for i := uint64(0); i < con; i++ {
		eg.Go(func() error {
			for {
				iter := client.Single().QueryWithOptions(ctx, spanner.Statement{SQL: query}, spanner.QueryOptions{Priority: priority})
				for {
					_, err := iter.Next()

					if err == nil {
						break
					} else {
						return err
					}

					code := status.Code(err)
					if code == codes.DeadlineExceeded {
						return nil
					}
				}
				iter.Stop()
			}
			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		code := status.Code(err)
		if err != context.DeadlineExceeded && code != codes.DeadlineExceeded {
			log.Fatal(err)
		}
	}
}
