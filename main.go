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

			if _, err := tryClient(ctx, c, "SELECT version();"); err != nil {
				return fmt.Errorf("failed ezpz query: %w", err)
			}

			if v, err := tryClient(ctx, c, os.Getenv("PSQL_QUERY")); err != nil {
				return err
			} else {
				log.Printf("%q = %v", os.Getenv("PSQL_QUERY"), v)
			}

			return nil
		}(); err != nil {
			log.Printf("failed to use a new client: %v", err)
		}

		if _, err := tryClient(ctx, stickyClient, "SELECT version();"); err != nil {
			log.Printf("failed ezpz query with a sticky client: %v", err)
		} else if v, err := tryClient(ctx, stickyClient, os.Getenv("PSQL_QUERY")); err != nil {
			log.Printf("failed to use a sticky client: %v", err)
		} else {
			log.Printf("%q = %v", os.Getenv("PSQL_QUERY"), v)
		}

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

	}
}

func tryClient(ctx context.Context, c *sql.DB, q string) (interface{}, error) {
	tx, err := c.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	result := make(chan interface{})
	errc := make(chan error)

	go func() {
		defer close(result)
		defer close(errc)

		v, err := func() (interface{}, error) {
			var version string
			if err := tx.QueryRowContext(ctx, "SELECT version();").Scan(&version); err != nil {
				return nil, fmt.Errorf("failed ezpz query: %w", err)
			}

			var v interface{}
			if err := tx.QueryRowContext(ctx, q).Scan(&v); err != nil {
				return nil, fmt.Errorf("failed to query row context(%q): %w", q, err)
			}

			if v == nil {
				return nil, fmt.Errorf("no value scanned (%q)", q)
			}

			if err := tx.Commit(); err != nil {
				return nil, err
			}

			return v, nil
		}()
		if err != nil {
			select {
			case errc <- err:
			case <-ctx.Done():
			}
		} else {
			select {
			case result <- v:
			case <-ctx.Done():
			}
		}
	}()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case err := <-errc:
		return nil, err
	case v := <-result:
		return v, nil
	}
}
