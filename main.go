package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	_ "github.com/lib/pq"
)

var (
	PSQLConnString      = os.Getenv("PSQL_CONN_STRING")
	QueryInterval       = mustParseDuration(envOr("QUERY_INTERVAL", "3s"))
	TransactionDuration = mustParseDuration(envOr("TRANSACTION_DURATION", "0"))
	PingPongInterval    = mustParseDuration(envOr("PING_PONG_INTERVAL", "0"))
	PSQLQuery           = envOr("PSQL_QUERY", "SELECT version();")
	FreshClient         = envOr("FRESH_CLIENT", "true") != "false"
	StickyClient        = envOr("STICKY_CLIENT", "false") != "false"
	NoClose             = envOr("NO_CLOSE", "false") == "true"
	N                   = mustParseInt(envOr("N", "0"))
	Quiet               = envOr("QUIET", "false") == "true"
)

func mustParseInt(s string) int {
	d, err := strconv.Atoi(s)
	if err != nil {
		panic(err)
	}
	return d
}

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

	for i := 0; i < N-1; i++ {
		i := i
		c, err := sql.Open("postgres", PSQLConnString)
		if err != nil {
			panic(err)
		}
		defer func() {
			if NoClose {
				return
			}
			if err := c.Close(); err != nil {
				log.Printf("failed to close client[%d]: %v", i, err)
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
	if _, _, err := tryQuery(ctx, c, "SELECT version();", 0); err != nil {
		return fmt.Errorf("failed to use client at all: %w", err)
	}

	result, duration, err := tryQuery(ctx, c, PSQLQuery, TransactionDuration)

	if _, _, err := tryQuery(ctx, c, "SELECT version();", 0); err != nil {
		return fmt.Errorf("failed to use client again: %w", err)
	}

	if Quiet {
		if err == nil {
			fmt.Printf("\r😃")
		} else {
			fmt.Printf("\r🫥")
		}
	} else if err != nil {
		return fmt.Errorf("failed to use client (%s): %w", PSQLQuery, err)
	} else {
		log.Printf("(%fms) %q = %v", float64(duration)/float64(time.Millisecond), PSQLQuery, result)
	}

	return nil
}

func tryQuery(ctx context.Context, c *sql.DB, q string, sleep time.Duration) (interface{}, time.Duration, error) {
	if sleep > 0 {
		return tryQueryAsTX(ctx, c, q, sleep)
	}
	return _tryQuery(ctx, c, q)
}

func tryQueryAsTX(ctx context.Context, c *sql.DB, q string, sleep time.Duration) (interface{}, time.Duration, error) {
	tx, err := c.BeginTx(ctx, nil)
	if err != nil {
		return nil, 0, err
	}
	defer tx.Rollback()

	var version string
	if err := tx.QueryRowContext(ctx, "SELECT version();").Scan(&version); err != nil {
		return nil, 0, fmt.Errorf("failed ezpz query: %w", err)
	}

	select {
	case <-ctx.Done():
		return nil, 0, ctx.Err()
	case <-time.After(sleep):
	}

	start := time.Now()
	var v interface{}
	if err := tx.QueryRowContext(ctx, q).Scan(&v); err != nil {
		return nil, 0, fmt.Errorf("failed to query row context(%q): %w", q, err)
	} else if v == nil {
		return nil, 0, fmt.Errorf("no value scanned (%q)", q)
	}
	duration := time.Since(start)

	return v, duration, tx.Commit()
}

func _tryQuery(ctx context.Context, c *sql.DB, q string) (interface{}, time.Duration, error) {
	var version string
	row := c.QueryRowContext(ctx, "SELECT version();")
	if err := row.Scan(&version); err != nil {
		return nil, 0, fmt.Errorf("failed ezpz query: %w", err)
	}
	if err := row.Err(); err != nil {
		return nil, 0, fmt.Errorf("failed ezpz query: %w", err)
	}

	var v interface{}
	start := time.Now()
	row = c.QueryRowContext(ctx, q)
	if err := row.Scan(&v); err != nil {
		return nil, 0, fmt.Errorf("failed to scan row context(%q): %w", q, err)
	} else if v == nil {
		return nil, 0, fmt.Errorf("no value scanned (%q)", q)
	}
	return v, time.Since(start), row.Err()
}
