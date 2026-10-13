package main

import (
	"context"
	"database/sql"
	"flag"
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
	NQuery              = envOr("N_QUERY", "false") == "true"
	Quiet               = envOr("QUIET", "false") == "true"
	Port                = mustParseInt(envOr("PORT", "10000"))
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
	fs := flag.NewFlagSet(os.Args[0], flag.ContinueOnError)
	if err := fs.Parse(os.Args[1:]); err != nil {
		panic(err)
	}

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

	var last, last2 string
	go func() {
		for ctx.Err() == nil {
			time.Sleep(time.Second)
			http.ListenAndServe(fmt.Sprintf(":%d", Port), http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				fmt.Fprintln(w, last, last2)
			}))
		}
	}()

	ticker := time.NewTicker(QueryInterval)
	defer ticker.Stop()

	var stickyClient *sql.DB
	var err error
	if StickyClient {
		log.Printf("dialing sticky...")
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
		if NQuery {
			go func(i int) {
				ticker := time.NewTicker(QueryInterval)
				defer ticker.Stop()

				for {
					GoN(i)
					select {
					case <-ctx.Done():
						return
					case <-ticker.C:
					}
				}
			}(i)
		} else {
			i := i
			log.Printf("dialing async...")
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
	}

	for {
		if FreshClient {
			d := GoN(0)
			if !Quiet && d > 0 {
				log.Printf("(%fms) fresh dial + query", float64(d)/float64(time.Millisecond))
			}
		}

		if stickyClient != nil {
			if err := tryClient(ctx, stickyClient, &last); err != nil {
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

func tryClient(ctx context.Context, c *sql.DB, last *string) error {
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
		*last = fmt.Sprintf("failed to use client (%s): %v", PSQLQuery, err)
		return fmt.Errorf("%s", *last)
	} else {
		*last = fmt.Sprintf("(%fms) %q = %v", float64(duration)/float64(time.Millisecond), PSQLQuery, result)
		log.Printf("%s", *last)
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

func GoN(i int) time.Duration {
	var d time.Duration
	if err := func() error {
		start := time.Now()

		ctx, can := context.WithTimeout(ctx, 30*time.Second)
		defer can()

		log.Printf("dialing fresh...")
		c, err := sql.Open("postgres", PSQLConnString)
		if err != nil {
			return err
		}
		log.Printf("pinging fresh...")
		if err := c.PingContext(ctx); err != nil {
			return err
		}
		log.Printf("trying fresh...")

		defer func() {
			if NoClose {
				return
			}
			if err := c.Close(); err != nil {
				log.Printf("failed to close fresh client: %v", err)
			}
		}()

		if err := tryClient(ctx, c, &last); err != nil {
			return err
		}

		d = time.Since(start)

		return nil
	}(); err != nil {
		log.Printf("failed to use a new client: %v", err)
	}
	return d
}
