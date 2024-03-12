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

var (
	QueryInterval = func() time.Duration {
		intervalS := os.Getenv("QUERY_INTERVAL")
		if intervalS == "" {
			return time.Second * 3
		}
		d, err := time.ParseDuration(intervalS)
		if err != nil {
			panic(err)
		}
		return d
	}()
	TransactionDuration = func() time.Duration {
		durationS := os.Getenv("TRANSACTION_DURATION")
		if durationS == "" {
			return 0
		}
		d, err := time.ParseDuration(durationS)
		if err != nil {
			panic(err)
		}
		return d
	}()
	PSQLConnString = os.Getenv("PSQL_CONN_STRING")
	PSQLQuery      = os.Getenv("PSQL_QUERY")
	FreshClient    = os.Getenv("FRESH_CLIENT") != "false"
	StickyClient   = os.Getenv("STICKY_CLIENT") != "false"
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

	ticker := time.NewTicker(QueryInterval)
	defer ticker.Stop()

	stickyClient, err := sql.Open("postgres", PSQLConnString)
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := stickyClient.Close(); err != nil {
			log.Printf("failed to close sticky client: %v", err)
		}
	}()

	for {
		if FreshClient {
			if err := func() error {
				c, err := sql.Open("postgres", PSQLConnString)
				if err != nil {
					return err
				}
				defer func() {
					if err := c.Close(); err != nil {
						log.Printf("failed to close fresh client: %v", err)
					}
				}()

				if _, err := tryClient(ctx, c, "SELECT version();"); err != nil {
					return fmt.Errorf("failed ezpz query: %w", err)
				}

				if v, err := tryClient(ctx, c, PSQLQuery); err != nil {
					return err
				} else {
					log.Printf("%q = %v", PSQLQuery, v)
				}

				return nil
			}(); err != nil {
				log.Printf("failed to use a new client: %v", err)
			}
		}

		if StickyClient {
			if _, err := tryClient(ctx, stickyClient, "SELECT version();"); err != nil {
				log.Printf("failed ezpz query with a sticky client: %v", err)
			} else if v, err := tryClient(ctx, stickyClient, PSQLQuery); err != nil {
				log.Printf("failed to use a sticky client: %v", err)
			} else {
				log.Printf("%q = %v", PSQLQuery, v)
			}
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

	var version string
	if err := tx.QueryRowContext(ctx, "SELECT version();").Scan(&version); err != nil {
		return nil, fmt.Errorf("failed ezpz query: %w", err)
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(TransactionDuration):
	}

	var v interface{}
	if err := tx.QueryRowContext(ctx, q).Scan(&v); err != nil {
		return nil, fmt.Errorf("failed to query row context(%q): %w", q, err)
	} else if v == nil {
		return nil, fmt.Errorf("no value scanned (%q)", q)
	}

	return v, tx.Commit()
}
