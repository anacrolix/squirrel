module github.com/anacrolix/squirrel

go 1.21

require (
	github.com/ajwerner/btree v0.0.0-20211221152037-f427b3e689c0
	github.com/alexflint/go-arg v1.4.3
	github.com/anacrolix/envpprof v1.3.0
	github.com/anacrolix/generics v0.0.0-20230816105729-c755655aee45
	github.com/anacrolix/log v0.14.3-0.20230823030427-4b296d71a6b4
	github.com/anacrolix/sync v0.4.1-0.20230926072150-b8cd7cfb92d0
	github.com/frankban/quicktest v1.14.6
	github.com/go-llsqlite/adapter v0.0.0-20230927005056-7f5ce7f0c916
	golang.org/x/sync v0.3.0
)

require (
	github.com/alexflint/go-scalar v1.1.0 // indirect
	github.com/anacrolix/chansync v0.3.0 // indirect
	github.com/anacrolix/missinggo v1.2.1 // indirect
	github.com/anacrolix/missinggo/perf v1.0.0 // indirect
	github.com/dustin/go-humanize v1.0.0 // indirect
	github.com/go-llsqlite/crawshaw v0.4.0 // indirect
	github.com/google/go-cmp v0.5.9 // indirect
	github.com/google/uuid v1.3.0 // indirect
	github.com/huandu/xstrings v1.2.0 // indirect
	github.com/kr/pretty v0.3.1 // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/mattn/go-isatty v0.0.16 // indirect
	github.com/remyoudompheng/bigfft v0.0.0-20230129092748-24d4a6f8daec // indirect
	github.com/rogpeppe/go-internal v1.9.0 // indirect
	golang.org/x/exp v0.0.0-20230626212559-97b1e661b5df // indirect
	golang.org/x/sys v0.11.0 // indirect
	modernc.org/libc v1.22.3 // indirect
	modernc.org/mathutil v1.5.0 // indirect
	modernc.org/memory v1.5.0 // indirect
	modernc.org/sqlite v1.21.1 // indirect
	zombiezen.com/go/sqlite v0.13.1 // indirect
)

retract (
	// Contains retractions only
	v0.6.2
	// Bad Cache.Create
	v0.6.0
)

