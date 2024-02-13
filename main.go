package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "github.com/lib/pq"
)

func main() {
	ctx, can := signal.NotifyContext(context.Background(), syscall.SIGINT)
	defer can()

	ticker := time.NewTicker(time.Second * 3)
	defer ticker.Stop()

	stickyClient, err := sql.Open("postgres", os.Getenv("PSQL_CONN_STRING"))
	if err != nil {
		panic(err)
	}
	defer stickyClient.Close()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

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
	}
}

func tryClient(ctx context.Context, c *sql.DB) error {
	rows, err := c.Query("SELECT version() as v;")
	if err != nil {
		return err
	}
	defer rows.Close()

	var v string
	for rows.Next() {
		if err := rows.Scan(&v); err != nil {
			return err
		}
	}
	if v == "" {
		return fmt.Errorf("failed to query version: seemingly empty %q", v)
	}
	log.Printf("SELECT version() = %q", v)

	return rows.Err()
}
