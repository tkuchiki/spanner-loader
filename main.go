package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/kelseyhightower/envconfig"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Config struct {
	GCPProjectID    string `envconfig:"GCP_PROJECT_ID" required:"true"`
	SpannerInstance string `envconfig:"SPANNER_INSTANCE" required:"true"`
	SpannerDatabase string `envconfig:"SPANNER_DATABASE" required:"true"`
}

func dbpath(projectID, instanceName, databaseName string) string {
	return fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectID, instanceName, databaseName)
}

func newSpannerClient(ctx context.Context, projectID, instanceName, databaseName string, config spanner.ClientConfig) (*spanner.Client, error) {
	return spanner.NewClientWithConfig(ctx, dbpath(projectID, instanceName, databaseName), config)
}

func main() {
	var con uint64
	var duration time.Duration
	var query string
	var project string
	var instance string
	var db string
	flag.Uint64Var(&con, "c", 1, "Concurrency")
	flag.DurationVar(&duration, "d", 60*time.Second, "Duration")
	flag.StringVar(&query, "query", "", "SQL")
	flag.StringVar(&project, "project", "", "GCP Project")
	flag.StringVar(&instance, "instance", "", "Cloud Spanner Instance")
	flag.StringVar(&db, "database", "", "Cloud Spanner Database")
	flag.Parse()

	if query == "" {
		log.Fatal("-query is required")
	}

	var c Config
	err := envconfig.Process("spanner-loader", &c)
	if err != nil {
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
				iter := client.Single().Query(ctx, spanner.Statement{SQL: query})
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
