package main

import (
	"fmt"
	"log"
	"os"

	"github.com/alexflint/go-arg"
	"github.com/go-llsqlite/adapter"

	"github.com/anacrolix/squirrel"
)

type InitCommand struct {
	Path string `arg:"positional"`
}

func main() {
	err := mainErr()
	if err != nil {
		log.Printf("error in main: %v", err)
		os.Exit(1)
	}
}

func mainErr() error {
	var args struct {
		Init *InitCommand `arg:"subcommand"`
	}
	p := arg.MustParse(&args)
	switch {
	case args.Init != nil:
		conn, err := sqlite.OpenConn(args.Init.Path, 0)
		if err != nil {
			return fmt.Errorf("opening sqlite conn: %w", err)
		}
		defer conn.Close()
		return squirrel.InitSchema(conn, 1<<14, true)
	default:
		p.Fail("expected subcommand")
		panic("unreachable")
	}
}
