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
	QueryInterval       = mustParseDuration(envOr("QUERY_INTERVAL", "3s"))
	TransactionDuration = mustParseDuration(envOr("TRANSACTION_DURATION", "0"))
	PingPongInterval    = mustParseDuration(envOr("PING_PONG_INTERVAL", "0"))
	PSQLConnString      = os.Getenv("PSQL_CONN_STRING")
	PSQLQuery           = os.Getenv("PSQL_QUERY")
	FreshClient         = os.Getenv("FRESH_CLIENT") != "false"
	StickyClient        = os.Getenv("STICKY_CLIENT") != "false"
	NoClose             = os.Getenv("NO_CLOSE") == "true"
)

func mustParseDuration(s string) time.Duration {
	d, err := time.ParseDuration(s)
	if err != nil {
		panic(err)
	}
	return d
}

func envOr(k, v string) string {
	v2 := os.Getenv(k)
	if v2 == "" {
		return v
	}
	return v2
}

func main() {
	ctx, can := signal.NotifyContext(context.Background(), syscall.SIGINT)
	defer can()

	if PingPongInterval > 0 {
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				case <-time.After(PingPongInterval):
					log.Println("ping")
				}
			}
		}()
	}

	go func() {
		for ctx.Err() == nil {
			time.Sleep(time.Second)
			http.ListenAndServe(":8080", http.HandlerFunc(http.NotFound))
		}
	}()

	ticker := time.NewTicker(QueryInterval)
	defer ticker.Stop()

	var stickyClient *sql.DB
	var err error
	if StickyClient {
		stickyClient, err = sql.Open("postgres", PSQLConnString)
		if err != nil {
			panic(err)
		}
		defer func() {
			if NoClose {
				return
			}
			if err := stickyClient.Close(); err != nil {
				log.Printf("failed to close sticky client: %v", err)
			}
		}()
	}

	for {
		if FreshClient {
			if err := func() error {
				c, err := sql.Open("postgres", PSQLConnString)
				if err != nil {
					return err
				}
				defer func() {
					if NoClose {
						return
					}
					if err := c.Close(); err != nil {
						log.Printf("failed to close fresh client: %v", err)
					}
				}()

				return tryClient(ctx, c)
			}(); err != nil {
				log.Printf("failed to use a new client: %v", err)
			}
		}

		if stickyClient != nil {
			if err := tryClient(ctx, stickyClient); err != nil {
				log.Printf("failed to use a sticky client: %v", err)
			}
		}

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

	}
}

func tryClient(ctx context.Context, c *sql.DB) error {
	if _, err := tryQuery(ctx, c, "SELECT version();", 0); err != nil {
		return fmt.Errorf("failed to use client at all: %w", err)
	}

	result, err := tryQuery(ctx, c, PSQLQuery, TransactionDuration)

	if _, err := tryQuery(ctx, c, "SELECT version();", 0); err != nil {
		return fmt.Errorf("failed to use client again: %w", err)
	}

	if err != nil {
		return fmt.Errorf("failed to use client (%s): %w", PSQLQuery, err)
	}

	log.Printf("%q = %v", PSQLQuery, result)
	return nil
}

func tryQuery(ctx context.Context, c *sql.DB, q string, sleep time.Duration) (interface{}, error) {
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
	case <-time.After(sleep):
	}

	var v interface{}
	if err := tx.QueryRowContext(ctx, q).Scan(&v); err != nil {
		return nil, fmt.Errorf("failed to query row context(%q): %w", q, err)
	} else if v == nil {
		return nil, fmt.Errorf("no value scanned (%q)", q)
	}

	return v, tx.Commit()
}
