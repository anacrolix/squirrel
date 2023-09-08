package squirrel

import (
	"errors"
	"fmt"
	g "github.com/anacrolix/generics"
	sqlite "github.com/go-llsqlite/adapter"
	"github.com/go-llsqlite/adapter/sqlitex"
)

type ErrUnexpectedJournalMode struct {
	JournalMode string
}

func (me ErrUnexpectedJournalMode) Error() string {
	return fmt.Sprintf("unexpected journal mode: %q", me.JournalMode)
}

func setSynchronous(conn conn, syncInt int) (err error) {
	err = sqlitex.ExecTransient(conn, fmt.Sprintf(`pragma synchronous=%v`, syncInt), nil)
	if err != nil {
		return err
	}
	var (
		actual   int
		actualOk bool
	)
	err = sqlitex.ExecTransient(conn, `pragma synchronous`, func(stmt *sqlite.Stmt) error {
		actual = stmt.ColumnInt(0)
		actualOk = true
		return nil
	})
	if err != nil {
		return
	}
	if !actualOk {
		return errors.New("synchronous setting query didn't return anything")
	}
	if actual != syncInt {
		return fmt.Errorf("set synchronous %q, got %q", syncInt, actual)
	}
	return nil
}

func setAndVerifyPragma(conn conn, name string, value any) (err error) {
	return setAndMaybeVerifyPragma(conn, name, value, g.Some(value))
}

func setAndMaybeVerifyPragma(conn conn, name string, value any, verify g.Option[any]) (err error) {
	valueStr := fmt.Sprint(value)
	setPragmaQuery := fmt.Sprintf("pragma %s=%s", name, valueStr)
	text, err := execTransientReturningText(conn, setPragmaQuery)
	if err != nil {
		return
	}
	if !verify.Ok {
		return
	}
	if !text.Ok {
		text, err = execTransientReturningText(conn, fmt.Sprintf("pragma %s", name))
		if err != nil {
			return
		}
	}
	if !text.Ok {
		err = errors.New("pragma did not return value")
		return
	}
	expectedText := fmt.Sprint(verify.Value)
	actualText := text.Value
	if actualText != expectedText {
		err = fmt.Errorf("%q returned %q", setPragmaQuery, actualText)
	}
	return
}

func execTransientReturningText(conn conn, query string) (s g.Option[string], err error) {
	var once setOnce[string]
	err = sqlitex.ExecTransient(conn, query, func(stmt *sqlite.Stmt) error {
		once.Set(stmt.ColumnText(0))
		return nil
	})
	if once.Ok() {
		s.Set(once.Value())
	}
	return
}
