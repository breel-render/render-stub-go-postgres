package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "github.com/lib/pq"
)

func main() {
	ctx, can := signal.NotifyContext(context.Background(), syscall.SIGINT)
	defer can()

	go func() {
		for ctx.Err() == nil {
			time.Sleep(time.Second)
			http.ListenAndServe(":8080", http.HandlerFunc(http.NotFound))
		}
	}()

	interval := time.Second * 3
	if intervalS := os.Getenv("PSQL_QUERY_INTERVAL"); intervalS != "" {
		d, err := time.ParseDuration(intervalS)
		if err != nil {
			panic(err)
		}
		interval = d
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	stickyClient, err := sql.Open("postgres", os.Getenv("PSQL_CONN_STRING"))
	if err != nil {
		panic(err)
	}
	defer stickyClient.Close()

	for {
		if err := func() error {
			c, err := sql.Open("postgres", os.Getenv("PSQL_CONN_STRING"))
			if err != nil {
				return err
			}
			defer c.Close()

			return tryClient(ctx, c)
		}(); err != nil {
			log.Printf("failed to use a new client: %v", err)
		}

		if err := tryClient(ctx, stickyClient); err != nil {
			log.Printf("failed to use a sticky client: %v", err)
		}

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

	}
}

func tryClient(ctx context.Context, c *sql.DB) error {
	tx, err := c.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	var v interface{}
	if err := tx.QueryRowContext(ctx, os.Getenv("PSQL_QUERY")).Scan(&v); err != nil {
		return fmt.Errorf("failed to query row context(%q): %w", os.Getenv("PSQL_QUERY"), err)
	}

	if v == nil {
		return fmt.Errorf("no value scanned (%q)", os.Getenv("PSQL_QUERY"))
	}

	log.Printf("%q = %v", os.Getenv("PSQL_QUERY"), v)
	return tx.Commit()
}
