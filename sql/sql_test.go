package sql

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/sjcotto/s3lite/engine"
)

// --- Lexer tests ---

func TestLexerBasicTokens(t *testing.T) {
	input := "SELECT * FROM users WHERE id = 1"
	lex := NewLexer(input)
	tokens, err := lex.Tokenize()
	if err != nil {
		t.Fatalf("Tokenize: %v", err)
	}

	expected := []TokenType{TokSelect, TokStar, TokFrom, TokIdent, TokWhere, TokIdent, TokEq, TokNumber, TokEOF}
	if len(tokens) != len(expected) {
		t.Fatalf("expected %d tokens, got %d", len(expected), len(tokens))
	}
	for i, e := range expected {
		if tokens[i].Type != e {
			t.Fatalf("token[%d]: expected type %v, got %v (%q)", i, e, tokens[i].Type, tokens[i].Value)
		}
	}
}

func TestLexerStringLiteral(t *testing.T) {
	input := "INSERT INTO t VALUES ('hello world')"
	lex := NewLexer(input)
	tokens, err := lex.Tokenize()
	if err != nil {
		t.Fatal(err)
	}

	// Find the string token.
	found := false
	for _, tok := range tokens {
		if tok.Type == TokString && tok.Value == "hello world" {
			found = true
		}
	}
	if !found {
		t.Fatal("expected string literal 'hello world' in tokens")
	}
}

func TestLexerEscapedQuote(t *testing.T) {
	input := "'it''s'"
	lex := NewLexer(input)
	tokens, err := lex.Tokenize()
	if err != nil {
		t.Fatal(err)
	}
	if tokens[0].Type != TokString || tokens[0].Value != "it's" {
		t.Fatalf("expected string \"it's\", got %q", tokens[0].Value)
	}
}

func TestLexerOperators(t *testing.T) {
	input := "a <= b >= c != d <> e"
	lex := NewLexer(input)
	tokens, err := lex.Tokenize()
	if err != nil {
		t.Fatal(err)
	}

	ops := []TokenType{TokIdent, TokLte, TokIdent, TokGte, TokIdent, TokNeq, TokIdent, TokNeq, TokIdent, TokEOF}
	if len(tokens) != len(ops) {
		t.Fatalf("expected %d tokens, got %d", len(ops), len(tokens))
	}
	for i, e := range ops {
		if tokens[i].Type != e {
			t.Fatalf("token[%d]: expected %v, got %v (%q)", i, e, tokens[i].Type, tokens[i].Value)
		}
	}
}

func TestLexerFloatNumber(t *testing.T) {
	input := "3.14"
	lex := NewLexer(input)
	tokens, err := lex.Tokenize()
	if err != nil {
		t.Fatal(err)
	}
	if tokens[0].Type != TokNumber || tokens[0].Value != "3.14" {
		t.Fatalf("expected number '3.14', got %v %q", tokens[0].Type, tokens[0].Value)
	}
}

func TestLexerNegativeNumber(t *testing.T) {
	input := "-42"
	lex := NewLexer(input)
	tokens, err := lex.Tokenize()
	if err != nil {
		t.Fatal(err)
	}
	if tokens[0].Type != TokNumber || tokens[0].Value != "-42" {
		t.Fatalf("expected number '-42', got %v %q", tokens[0].Type, tokens[0].Value)
	}
}

func TestLexerKeywords(t *testing.T) {
	input := "CREATE TABLE DELETE UPDATE SET ORDER BY ASC DESC LIMIT"
	lex := NewLexer(input)
	tokens, err := lex.Tokenize()
	if err != nil {
		t.Fatal(err)
	}

	expected := []TokenType{TokCreate, TokTable, TokDelete, TokUpdate, TokSet, TokOrder, TokBy, TokAsc, TokDesc, TokLimit, TokEOF}
	for i, e := range expected {
		if tokens[i].Type != e {
			t.Fatalf("token[%d]: expected %v, got %v (%q)", i, e, tokens[i].Type, tokens[i].Value)
		}
	}
}

func TestLexerUnterminatedString(t *testing.T) {
	input := "'unterminated"
	lex := NewLexer(input)
	_, err := lex.Tokenize()
	if err == nil {
		t.Fatal("expected error for unterminated string")
	}
}

func TestLexerQuotedIdent(t *testing.T) {
	input := `"my column"`
	lex := NewLexer(input)
	tokens, err := lex.Tokenize()
	if err != nil {
		t.Fatal(err)
	}
	if tokens[0].Type != TokIdent || tokens[0].Value != "my column" {
		t.Fatalf("expected ident 'my column', got %v %q", tokens[0].Type, tokens[0].Value)
	}
}

// --- Parser tests ---

func TestParseCreateTable(t *testing.T) {
	stmt, err := ParseSQL("CREATE TABLE users (id INT PRIMARY KEY, name TEXT, age INT)")
	if err != nil {
		t.Fatalf("ParseSQL: %v", err)
	}

	ct, ok := stmt.(*CreateTableStmt)
	if !ok {
		t.Fatalf("expected CreateTableStmt, got %T", stmt)
	}
	if ct.Name != "users" {
		t.Fatalf("expected table name 'users', got %q", ct.Name)
	}
	if len(ct.Columns) != 3 {
		t.Fatalf("expected 3 columns, got %d", len(ct.Columns))
	}
	if !ct.Columns[0].PK {
		t.Fatal("first column should be PK")
	}
	if ct.Columns[0].Type != "int" {
		t.Fatalf("first column type should be 'int', got %q", ct.Columns[0].Type)
	}
}

func TestParseCreateTableIfNotExists(t *testing.T) {
	stmt, err := ParseSQL("CREATE TABLE IF NOT EXISTS t1 (id INT PRIMARY KEY)")
	if err != nil {
		t.Fatal(err)
	}
	ct := stmt.(*CreateTableStmt)
	if !ct.IfNotExists {
		t.Fatal("expected IfNotExists = true")
	}
}

func TestParseInsert(t *testing.T) {
	stmt, err := ParseSQL("INSERT INTO users (id, name) VALUES (1, 'Alice')")
	if err != nil {
		t.Fatalf("ParseSQL: %v", err)
	}

	ins, ok := stmt.(*InsertStmt)
	if !ok {
		t.Fatalf("expected InsertStmt, got %T", stmt)
	}
	if ins.Table != "users" {
		t.Fatalf("expected table 'users', got %q", ins.Table)
	}
	if len(ins.Columns) != 2 {
		t.Fatalf("expected 2 columns, got %d", len(ins.Columns))
	}
	if len(ins.Values) != 1 || len(ins.Values[0]) != 2 {
		t.Fatalf("expected 1 row of 2 values, got %d rows", len(ins.Values))
	}
}

func TestParseInsertMultipleRows(t *testing.T) {
	stmt, err := ParseSQL("INSERT INTO t (a) VALUES (1), (2), (3)")
	if err != nil {
		t.Fatal(err)
	}
	ins := stmt.(*InsertStmt)
	if len(ins.Values) != 3 {
		t.Fatalf("expected 3 value rows, got %d", len(ins.Values))
	}
}

func TestParseSelect(t *testing.T) {
	stmt, err := ParseSQL("SELECT id, name FROM users WHERE age > 18 ORDER BY name DESC LIMIT 10")
	if err != nil {
		t.Fatalf("ParseSQL: %v", err)
	}

	sel, ok := stmt.(*SelectStmt)
	if !ok {
		t.Fatalf("expected SelectStmt, got %T", stmt)
	}
	if sel.From != "users" {
		t.Fatalf("expected FROM 'users', got %q", sel.From)
	}
	if len(sel.Columns) != 2 {
		t.Fatalf("expected 2 columns, got %d", len(sel.Columns))
	}
	if sel.Where == nil {
		t.Fatal("expected WHERE clause")
	}
	if len(sel.OrderBy) != 1 || sel.OrderBy[0].Column != "name" || !sel.OrderBy[0].Desc {
		t.Fatalf("unexpected ORDER BY: %+v", sel.OrderBy)
	}
	if sel.Limit != 10 || !sel.HasLimit {
		t.Fatalf("expected LIMIT 10, got %d", sel.Limit)
	}
}

func TestParseSelectStar(t *testing.T) {
	stmt, err := ParseSQL("SELECT * FROM t")
	if err != nil {
		t.Fatal(err)
	}
	sel := stmt.(*SelectStmt)
	if len(sel.Columns) != 1 || !sel.Columns[0].Star {
		t.Fatal("expected SELECT *")
	}
}

func TestParseSelectCountStar(t *testing.T) {
	stmt, err := ParseSQL("SELECT COUNT(*) FROM t")
	if err != nil {
		t.Fatal(err)
	}
	sel := stmt.(*SelectStmt)
	fn, ok := sel.Columns[0].Expr.(*FuncExpr)
	if !ok || fn.Name != "count" || !fn.Star {
		t.Fatal("expected COUNT(*)")
	}
}

func TestParseDelete(t *testing.T) {
	stmt, err := ParseSQL("DELETE FROM users WHERE id = 5")
	if err != nil {
		t.Fatal(err)
	}
	del, ok := stmt.(*DeleteStmt)
	if !ok {
		t.Fatalf("expected DeleteStmt, got %T", stmt)
	}
	if del.Table != "users" {
		t.Fatalf("expected table 'users', got %q", del.Table)
	}
	if del.Where == nil {
		t.Fatal("expected WHERE clause")
	}
}

func TestParseDeleteNoWhere(t *testing.T) {
	stmt, err := ParseSQL("DELETE FROM t")
	if err != nil {
		t.Fatal(err)
	}
	del := stmt.(*DeleteStmt)
	if del.Where != nil {
		t.Fatal("expected no WHERE clause")
	}
}

func TestParseUpdate(t *testing.T) {
	stmt, err := ParseSQL("UPDATE users SET name = 'Bob' WHERE id = 1")
	if err != nil {
		t.Fatal(err)
	}
	upd, ok := stmt.(*UpdateStmt)
	if !ok {
		t.Fatalf("expected UpdateStmt, got %T", stmt)
	}
	if upd.Table != "users" {
		t.Fatalf("expected table 'users', got %q", upd.Table)
	}
	if len(upd.Set) != 1 || upd.Set[0].Column != "name" {
		t.Fatalf("unexpected SET clause: %+v", upd.Set)
	}
	if upd.Where == nil {
		t.Fatal("expected WHERE clause")
	}
}

func TestParseDrop(t *testing.T) {
	stmt, err := ParseSQL("DROP TABLE IF EXISTS t")
	if err != nil {
		t.Fatal(err)
	}
	drop := stmt.(*DropTableStmt)
	if drop.Name != "t" || !drop.IfExists {
		t.Fatalf("unexpected: %+v", drop)
	}
}

func TestParseExprPrecedence(t *testing.T) {
	// Verify AND binds tighter than OR.
	stmt, err := ParseSQL("SELECT * FROM t WHERE a = 1 OR b = 2 AND c = 3")
	if err != nil {
		t.Fatal(err)
	}
	sel := stmt.(*SelectStmt)
	bin, ok := sel.Where.(*BinaryExpr)
	if !ok || bin.Op != "OR" {
		t.Fatalf("top-level should be OR, got %T %+v", sel.Where, sel.Where)
	}
}

// --- Executor tests (integration with engine) ---

func newTestExecutor(t *testing.T) *Executor {
	t.Helper()
	dir, err := os.MkdirTemp("", "s3lite-sql-test-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })

	cfg := engine.DefaultConfig(dir)
	cfg.CacheSize = 256
	e, err := engine.Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { e.Close(context.Background()) })

	return NewExecutor(e)
}

func TestExecutorCreateAndInsert(t *testing.T) {
	ex := newTestExecutor(t)
	ctx := context.Background()

	res, err := ex.Execute(ctx, "CREATE TABLE users (id INT PRIMARY KEY, name TEXT, age INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	if res.Message == "" {
		t.Fatal("expected message from CREATE TABLE")
	}

	res, err = ex.Execute(ctx, "INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30)")
	if err != nil {
		t.Fatalf("INSERT: %v", err)
	}
	if res.RowCount != 1 {
		t.Fatalf("expected 1 row inserted, got %d", res.RowCount)
	}

	res, err = ex.Execute(ctx, "INSERT INTO users (id, name, age) VALUES (2, 'Bob', 25), (3, 'Charlie', 35)")
	if err != nil {
		t.Fatalf("INSERT multi: %v", err)
	}
	if res.RowCount != 2 {
		t.Fatalf("expected 2 rows inserted, got %d", res.RowCount)
	}
}

func TestExecutorSelectAll(t *testing.T) {
	ex := newTestExecutor(t)
	ctx := context.Background()

	_, _ = ex.Execute(ctx, "CREATE TABLE items (id INT PRIMARY KEY, name TEXT)")
	_, _ = ex.Execute(ctx, "INSERT INTO items (id, name) VALUES (1, 'Apple')")
	_, _ = ex.Execute(ctx, "INSERT INTO items (id, name) VALUES (2, 'Banana')")
	_, _ = ex.Execute(ctx, "INSERT INTO items (id, name) VALUES (3, 'Cherry')")

	res, err := ex.Execute(ctx, "SELECT * FROM items")
	if err != nil {
		t.Fatalf("SELECT *: %v", err)
	}
	if res.RowCount != 3 {
		t.Fatalf("expected 3 rows, got %d", res.RowCount)
	}
	if len(res.Columns) != 2 {
		t.Fatalf("expected 2 columns, got %d", len(res.Columns))
	}
}

func TestExecutorSelectWhere(t *testing.T) {
	ex := newTestExecutor(t)
	ctx := context.Background()

	_, _ = ex.Execute(ctx, "CREATE TABLE nums (id INT PRIMARY KEY, val INT)")
	for i := 1; i <= 10; i++ {
		_, err := ex.Execute(ctx, fmt.Sprintf("INSERT INTO nums (id, val) VALUES (%d, %d)", i, i*10))
		if err != nil {
			t.Fatalf("insert %d: %v", i, err)
		}
	}

	res, err := ex.Execute(ctx, "SELECT * FROM nums WHERE val > 50")
	if err != nil {
		t.Fatal(err)
	}
	if res.RowCount != 5 {
		t.Fatalf("expected 5 rows where val > 50, got %d", res.RowCount)
	}
}

func TestExecutorSelectOrderBy(t *testing.T) {
	ex := newTestExecutor(t)
	ctx := context.Background()

	_, _ = ex.Execute(ctx, "CREATE TABLE people (id INT PRIMARY KEY, name TEXT)")
	_, _ = ex.Execute(ctx, "INSERT INTO people (id, name) VALUES (1, 'Charlie')")
	_, _ = ex.Execute(ctx, "INSERT INTO people (id, name) VALUES (2, 'Alice')")
	_, _ = ex.Execute(ctx, "INSERT INTO people (id, name) VALUES (3, 'Bob')")

	res, err := ex.Execute(ctx, "SELECT name FROM people ORDER BY name")
	if err != nil {
		t.Fatal(err)
	}
	if res.RowCount != 3 {
		t.Fatalf("expected 3 rows, got %d", res.RowCount)
	}

	names := make([]string, len(res.Rows))
	for i, row := range res.Rows {
		names[i] = fmt.Sprintf("%v", row[0])
	}
	expected := []string{"Alice", "Bob", "Charlie"}
	for i, e := range expected {
		if names[i] != e {
			t.Fatalf("row[%d]: expected %q, got %q", i, e, names[i])
		}
	}
}

func TestExecutorSelectLimit(t *testing.T) {
	ex := newTestExecutor(t)
	ctx := context.Background()

	_, _ = ex.Execute(ctx, "CREATE TABLE lim (id INT PRIMARY KEY)")
	for i := 1; i <= 20; i++ {
		_, _ = ex.Execute(ctx, fmt.Sprintf("INSERT INTO lim (id) VALUES (%d)", i))
	}

	res, err := ex.Execute(ctx, "SELECT * FROM lim LIMIT 5")
	if err != nil {
		t.Fatal(err)
	}
	if res.RowCount != 5 {
		t.Fatalf("expected 5 rows with LIMIT, got %d", res.RowCount)
	}
}

func TestExecutorCount(t *testing.T) {
	ex := newTestExecutor(t)
	ctx := context.Background()

	_, _ = ex.Execute(ctx, "CREATE TABLE cnt (id INT PRIMARY KEY)")
	for i := 1; i <= 7; i++ {
		_, _ = ex.Execute(ctx, fmt.Sprintf("INSERT INTO cnt (id) VALUES (%d)", i))
	}

	res, err := ex.Execute(ctx, "SELECT COUNT(*) FROM cnt")
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Rows) != 1 || len(res.Rows[0]) != 1 {
		t.Fatalf("expected 1 row with 1 column, got %v", res.Rows)
	}
	count, ok := res.Rows[0][0].(int64)
	if !ok {
		t.Fatalf("expected int64 count, got %T: %v", res.Rows[0][0], res.Rows[0][0])
	}
	if count != 7 {
		t.Fatalf("expected count 7, got %d", count)
	}
}

func TestExecutorDelete(t *testing.T) {
	ex := newTestExecutor(t)
	ctx := context.Background()

	_, _ = ex.Execute(ctx, "CREATE TABLE del (id INT PRIMARY KEY, val TEXT)")
	_, _ = ex.Execute(ctx, "INSERT INTO del (id, val) VALUES (1, 'a')")
	_, _ = ex.Execute(ctx, "INSERT INTO del (id, val) VALUES (2, 'b')")
	_, _ = ex.Execute(ctx, "INSERT INTO del (id, val) VALUES (3, 'c')")

	res, err := ex.Execute(ctx, "DELETE FROM del WHERE id = 2")
	if err != nil {
		t.Fatal(err)
	}
	if res.RowCount != 1 {
		t.Fatalf("expected 1 deleted, got %d", res.RowCount)
	}

	res, err = ex.Execute(ctx, "SELECT COUNT(*) FROM del")
	if err != nil {
		t.Fatal(err)
	}
	if res.Rows[0][0].(int64) != 2 {
		t.Fatalf("expected 2 remaining rows, got %v", res.Rows[0][0])
	}
}

func TestExecutorUpdate(t *testing.T) {
	ex := newTestExecutor(t)
	ctx := context.Background()

	_, _ = ex.Execute(ctx, "CREATE TABLE upd (id INT PRIMARY KEY, name TEXT)")
	_, _ = ex.Execute(ctx, "INSERT INTO upd (id, name) VALUES (1, 'old')")

	res, err := ex.Execute(ctx, "UPDATE upd SET name = 'new' WHERE id = 1")
	if err != nil {
		t.Fatal(err)
	}
	if res.RowCount != 1 {
		t.Fatalf("expected 1 updated, got %d", res.RowCount)
	}

	res, err = ex.Execute(ctx, "SELECT name FROM upd WHERE id = 1")
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(res.Rows))
	}
	if fmt.Sprintf("%v", res.Rows[0][0]) != "new" {
		t.Fatalf("expected name 'new', got %v", res.Rows[0][0])
	}
}

func TestExecutorDropTable(t *testing.T) {
	ex := newTestExecutor(t)
	ctx := context.Background()

	_, _ = ex.Execute(ctx, "CREATE TABLE todrop (id INT PRIMARY KEY)")
	res, err := ex.Execute(ctx, "DROP TABLE todrop")
	if err != nil {
		t.Fatal(err)
	}
	if res.Message == "" {
		t.Fatal("expected message from DROP")
	}

	// Selecting from dropped table should fail.
	_, err = ex.Execute(ctx, "SELECT * FROM todrop")
	if err == nil {
		t.Fatal("expected error selecting from dropped table")
	}
}

func TestExecutorSelectWhereAND(t *testing.T) {
	ex := newTestExecutor(t)
	ctx := context.Background()

	_, _ = ex.Execute(ctx, "CREATE TABLE multi (id INT PRIMARY KEY, a INT, b INT)")
	_, _ = ex.Execute(ctx, "INSERT INTO multi (id, a, b) VALUES (1, 10, 20)")
	_, _ = ex.Execute(ctx, "INSERT INTO multi (id, a, b) VALUES (2, 10, 30)")
	_, _ = ex.Execute(ctx, "INSERT INTO multi (id, a, b) VALUES (3, 20, 20)")

	res, err := ex.Execute(ctx, "SELECT * FROM multi WHERE a = 10 AND b = 20")
	if err != nil {
		t.Fatal(err)
	}
	if res.RowCount != 1 {
		t.Fatalf("expected 1 row, got %d", res.RowCount)
	}
}

func TestFormatResult(t *testing.T) {
	r := &Result{
		Columns: []string{"id", "name"},
		Rows: [][]interface{}{
			{int64(1), "Alice"},
			{int64(2), "Bob"},
		},
		RowCount: 2,
	}
	output := FormatResult(r)
	if output == "" {
		t.Fatal("FormatResult should not return empty string")
	}
	// Verify it contains column headers and row count.
	if !contains(output, "id") || !contains(output, "name") {
		t.Fatal("output should contain column names")
	}
	if !contains(output, "(2 rows)") {
		t.Fatal("output should contain row count")
	}
}

func TestFormatResultMessage(t *testing.T) {
	r := &Result{Message: "Table created"}
	output := FormatResult(r)
	if output != "Table created" {
		t.Fatalf("expected 'Table created', got %q", output)
	}
}

// --- New feature tests: Aggregates ---

func TestExecutorSum(t *testing.T) {
	ex := newTestExecutor(t)
	ctx := context.Background()

	_, _ = ex.Execute(ctx, "CREATE TABLE sales (id INT PRIMARY KEY, amount INT)")
	_, _ = ex.Execute(ctx, "INSERT INTO sales (id, amount) VALUES (1, 100)")
	_, _ = ex.Execute(ctx, "INSERT INTO sales (id, amount) VALUES (2, 200)")
	_, _ = ex.Execute(ctx, "INSERT INTO sales (id, amount) VALUES (3, 300)")

	res, err := ex.Execute(ctx, "SELECT SUM(amount) FROM sales")
	if err != nil {
		t.Fatalf("SUM: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(res.Rows))
	}
	sum, ok := res.Rows[0][0].(int64)
	if !ok {
		t.Fatalf("expected int64, got %T: %v", res.Rows[0][0], res.Rows[0][0])
	}
	if sum != 600 {
		t.Fatalf("expected SUM=600, got %d", sum)
	}
}

func TestExecutorAvg(t *testing.T) {
	ex := newTestExecutor(t)
	ctx := context.Background()

	_, _ = ex.Execute(ctx, "CREATE TABLE scores (id INT PRIMARY KEY, score INT)")
	_, _ = ex.Execute(ctx, "INSERT INTO scores (id, score) VALUES (1, 80)")
	_, _ = ex.Execute(ctx, "INSERT INTO scores (id, score) VALUES (2, 90)")
	_, _ = ex.Execute(ctx, "INSERT INTO scores (id, score) VALUES (3, 100)")

	res, err := ex.Execute(ctx, "SELECT AVG(score) FROM scores")
	if err != nil {
		t.Fatalf("AVG: %v", err)
	}
	avg, ok := res.Rows[0][0].(float64)
	if !ok {
		t.Fatalf("expected float64, got %T", res.Rows[0][0])
	}
	if avg != 90.0 {
		t.Fatalf("expected AVG=90, got %f", avg)
	}
}

func TestExecutorMinMax(t *testing.T) {
	ex := newTestExecutor(t)
	ctx := context.Background()

	_, _ = ex.Execute(ctx, "CREATE TABLE vals (id INT PRIMARY KEY, v INT)")
	_, _ = ex.Execute(ctx, "INSERT INTO vals (id, v) VALUES (1, 5)")
	_, _ = ex.Execute(ctx, "INSERT INTO vals (id, v) VALUES (2, 15)")
	_, _ = ex.Execute(ctx, "INSERT INTO vals (id, v) VALUES (3, 10)")

	res, err := ex.Execute(ctx, "SELECT MIN(v), MAX(v) FROM vals")
	if err != nil {
		t.Fatalf("MIN/MAX: %v", err)
	}
	if len(res.Rows) != 1 || len(res.Rows[0]) != 2 {
		t.Fatalf("expected 1 row with 2 cols, got %v", res.Rows)
	}

	minVal := toTestFloat(res.Rows[0][0])
	maxVal := toTestFloat(res.Rows[0][1])
	if minVal != 5 {
		t.Fatalf("expected MIN=5, got %v", minVal)
	}
	if maxVal != 15 {
		t.Fatalf("expected MAX=15, got %v", maxVal)
	}
}

func TestExecutorGroupBy(t *testing.T) {
	ex := newTestExecutor(t)
	ctx := context.Background()

	_, _ = ex.Execute(ctx, "CREATE TABLE orders (id INT PRIMARY KEY, category TEXT, amount INT)")
	_, _ = ex.Execute(ctx, "INSERT INTO orders (id, category, amount) VALUES (1, 'food', 100)")
	_, _ = ex.Execute(ctx, "INSERT INTO orders (id, category, amount) VALUES (2, 'food', 200)")
	_, _ = ex.Execute(ctx, "INSERT INTO orders (id, category, amount) VALUES (3, 'drink', 50)")
	_, _ = ex.Execute(ctx, "INSERT INTO orders (id, category, amount) VALUES (4, 'drink', 75)")

	res, err := ex.Execute(ctx, "SELECT category, SUM(amount) FROM orders GROUP BY category ORDER BY category")
	if err != nil {
		t.Fatalf("GROUP BY: %v", err)
	}
	if res.RowCount != 2 {
		t.Fatalf("expected 2 groups, got %d", res.RowCount)
	}

	// drink group: 50+75=125, food group: 100+200=300
	if fmt.Sprintf("%v", res.Rows[0][0]) != "drink" {
		t.Fatalf("expected first group 'drink', got %v", res.Rows[0][0])
	}
	if toTestFloat(res.Rows[0][1]) != 125 {
		t.Fatalf("expected drink sum=125, got %v", res.Rows[0][1])
	}
	if toTestFloat(res.Rows[1][1]) != 300 {
		t.Fatalf("expected food sum=300, got %v", res.Rows[1][1])
	}
}

func TestExecutorHaving(t *testing.T) {
	ex := newTestExecutor(t)
	ctx := context.Background()

	_, _ = ex.Execute(ctx, "CREATE TABLE products (id INT PRIMARY KEY, cat TEXT, price INT)")
	_, _ = ex.Execute(ctx, "INSERT INTO products (id, cat, price) VALUES (1, 'A', 10)")
	_, _ = ex.Execute(ctx, "INSERT INTO products (id, cat, price) VALUES (2, 'A', 20)")
	_, _ = ex.Execute(ctx, "INSERT INTO products (id, cat, price) VALUES (3, 'B', 5)")

	res, err := ex.Execute(ctx, "SELECT cat, SUM(price) FROM products GROUP BY cat HAVING SUM(price) > 10")
	if err != nil {
		t.Fatalf("HAVING: %v", err)
	}
	if res.RowCount != 1 {
		t.Fatalf("expected 1 group passing HAVING, got %d", res.RowCount)
	}
	if fmt.Sprintf("%v", res.Rows[0][0]) != "A" {
		t.Fatalf("expected group 'A', got %v", res.Rows[0][0])
	}
}

func TestExecutorCountWithGroupBy(t *testing.T) {
	ex := newTestExecutor(t)
	ctx := context.Background()

	_, _ = ex.Execute(ctx, "CREATE TABLE logs (id INT PRIMARY KEY, level TEXT)")
	_, _ = ex.Execute(ctx, "INSERT INTO logs (id, level) VALUES (1, 'info')")
	_, _ = ex.Execute(ctx, "INSERT INTO logs (id, level) VALUES (2, 'info')")
	_, _ = ex.Execute(ctx, "INSERT INTO logs (id, level) VALUES (3, 'error')")

	res, err := ex.Execute(ctx, "SELECT level, COUNT(*) FROM logs GROUP BY level ORDER BY level")
	if err != nil {
		t.Fatal(err)
	}
	if res.RowCount != 2 {
		t.Fatalf("expected 2 groups, got %d", res.RowCount)
	}
	// error: 1, info: 2
	if toTestFloat(res.Rows[0][1]) != 1 {
		t.Fatalf("expected error count=1, got %v", res.Rows[0][1])
	}
	if toTestFloat(res.Rows[1][1]) != 2 {
		t.Fatalf("expected info count=2, got %v", res.Rows[1][1])
	}
}

// --- JOIN tests ---

func TestExecutorInnerJoin(t *testing.T) {
	ex := newTestExecutor(t)
	ctx := context.Background()

	_, _ = ex.Execute(ctx, "CREATE TABLE users2 (id INT PRIMARY KEY, name TEXT)")
	_, _ = ex.Execute(ctx, "CREATE TABLE posts (id INT PRIMARY KEY, user_id INT, title TEXT)")

	_, _ = ex.Execute(ctx, "INSERT INTO users2 (id, name) VALUES (1, 'Alice')")
	_, _ = ex.Execute(ctx, "INSERT INTO users2 (id, name) VALUES (2, 'Bob')")

	_, _ = ex.Execute(ctx, "INSERT INTO posts (id, user_id, title) VALUES (1, 1, 'Hello')")
	_, _ = ex.Execute(ctx, "INSERT INTO posts (id, user_id, title) VALUES (2, 1, 'World')")
	_, _ = ex.Execute(ctx, "INSERT INTO posts (id, user_id, title) VALUES (3, 2, 'Hi')")

	res, err := ex.Execute(ctx, "SELECT users2.name, posts.title FROM users2 INNER JOIN posts ON users2.id = posts.user_id ORDER BY posts.title")
	if err != nil {
		t.Fatalf("INNER JOIN: %v", err)
	}
	if res.RowCount != 3 {
		t.Fatalf("expected 3 rows, got %d", res.RowCount)
	}
}

func TestExecutorLeftJoin(t *testing.T) {
	ex := newTestExecutor(t)
	ctx := context.Background()

	_, _ = ex.Execute(ctx, "CREATE TABLE dept (id INT PRIMARY KEY, name TEXT)")
	_, _ = ex.Execute(ctx, "CREATE TABLE emp (id INT PRIMARY KEY, dept_id INT, name TEXT)")

	_, _ = ex.Execute(ctx, "INSERT INTO dept (id, name) VALUES (1, 'Engineering')")
	_, _ = ex.Execute(ctx, "INSERT INTO dept (id, name) VALUES (2, 'Marketing')")
	_, _ = ex.Execute(ctx, "INSERT INTO dept (id, name) VALUES (3, 'Empty')")

	_, _ = ex.Execute(ctx, "INSERT INTO emp (id, dept_id, name) VALUES (1, 1, 'Alice')")
	_, _ = ex.Execute(ctx, "INSERT INTO emp (id, dept_id, name) VALUES (2, 2, 'Bob')")

	res, err := ex.Execute(ctx, "SELECT dept.name, emp.name FROM dept LEFT JOIN emp ON dept.id = emp.dept_id ORDER BY dept.name")
	if err != nil {
		t.Fatalf("LEFT JOIN: %v", err)
	}
	// Should have 3 rows: Empty (NULL emp), Engineering (Alice), Marketing (Bob)
	if res.RowCount != 3 {
		t.Fatalf("expected 3 rows from LEFT JOIN, got %d", res.RowCount)
	}
}

// --- IS NULL, IN, DISTINCT tests ---

func TestExecutorDistinct(t *testing.T) {
	ex := newTestExecutor(t)
	ctx := context.Background()

	_, _ = ex.Execute(ctx, "CREATE TABLE tags (id INT PRIMARY KEY, tag TEXT)")
	_, _ = ex.Execute(ctx, "INSERT INTO tags (id, tag) VALUES (1, 'go')")
	_, _ = ex.Execute(ctx, "INSERT INTO tags (id, tag) VALUES (2, 'go')")
	_, _ = ex.Execute(ctx, "INSERT INTO tags (id, tag) VALUES (3, 'rust')")

	res, err := ex.Execute(ctx, "SELECT DISTINCT tag FROM tags")
	if err != nil {
		t.Fatal(err)
	}
	if res.RowCount != 2 {
		t.Fatalf("expected 2 distinct tags, got %d", res.RowCount)
	}
}

func TestExecutorIn(t *testing.T) {
	ex := newTestExecutor(t)
	ctx := context.Background()

	_, _ = ex.Execute(ctx, "CREATE TABLE colors (id INT PRIMARY KEY, name TEXT)")
	_, _ = ex.Execute(ctx, "INSERT INTO colors (id, name) VALUES (1, 'red')")
	_, _ = ex.Execute(ctx, "INSERT INTO colors (id, name) VALUES (2, 'blue')")
	_, _ = ex.Execute(ctx, "INSERT INTO colors (id, name) VALUES (3, 'green')")

	res, err := ex.Execute(ctx, "SELECT * FROM colors WHERE name IN ('red', 'green')")
	if err != nil {
		t.Fatal(err)
	}
	if res.RowCount != 2 {
		t.Fatalf("expected 2 rows for IN, got %d", res.RowCount)
	}
}

func TestExecutorNotIn(t *testing.T) {
	ex := newTestExecutor(t)
	ctx := context.Background()

	_, _ = ex.Execute(ctx, "CREATE TABLE nums2 (id INT PRIMARY KEY)")
	for i := 1; i <= 5; i++ {
		_, _ = ex.Execute(ctx, fmt.Sprintf("INSERT INTO nums2 (id) VALUES (%d)", i))
	}

	res, err := ex.Execute(ctx, "SELECT * FROM nums2 WHERE id NOT IN (2, 4)")
	if err != nil {
		t.Fatal(err)
	}
	if res.RowCount != 3 {
		t.Fatalf("expected 3 rows for NOT IN, got %d", res.RowCount)
	}
}

func TestExecutorIsNull(t *testing.T) {
	ex := newTestExecutor(t)
	ctx := context.Background()

	_, _ = ex.Execute(ctx, "CREATE TABLE nullable (id INT PRIMARY KEY, val TEXT)")
	_, _ = ex.Execute(ctx, "INSERT INTO nullable (id, val) VALUES (1, 'a')")
	_, _ = ex.Execute(ctx, "INSERT INTO nullable (id, val) VALUES (2, NULL)")

	res, err := ex.Execute(ctx, "SELECT * FROM nullable WHERE val IS NULL")
	if err != nil {
		t.Fatal(err)
	}
	if res.RowCount != 1 {
		t.Fatalf("expected 1 row with NULL, got %d", res.RowCount)
	}

	res, err = ex.Execute(ctx, "SELECT * FROM nullable WHERE val IS NOT NULL")
	if err != nil {
		t.Fatal(err)
	}
	if res.RowCount != 1 {
		t.Fatalf("expected 1 row IS NOT NULL, got %d", res.RowCount)
	}
}

// --- Index tests ---

func TestExecutorCreateIndex(t *testing.T) {
	ex := newTestExecutor(t)
	ctx := context.Background()

	_, _ = ex.Execute(ctx, "CREATE TABLE indexed (id INT PRIMARY KEY, name TEXT, age INT)")
	_, _ = ex.Execute(ctx, "INSERT INTO indexed (id, name, age) VALUES (1, 'Alice', 30)")
	_, _ = ex.Execute(ctx, "INSERT INTO indexed (id, name, age) VALUES (2, 'Bob', 25)")
	_, _ = ex.Execute(ctx, "INSERT INTO indexed (id, name, age) VALUES (3, 'Charlie', 30)")

	res, err := ex.Execute(ctx, "CREATE INDEX idx_name ON indexed (name)")
	if err != nil {
		t.Fatalf("CREATE INDEX: %v", err)
	}
	if res.Message == "" {
		t.Fatal("expected message from CREATE INDEX")
	}

	// Query should still work (may use index)
	res, err = ex.Execute(ctx, "SELECT * FROM indexed WHERE name = 'Bob'")
	if err != nil {
		t.Fatal(err)
	}
	if res.RowCount != 1 {
		t.Fatalf("expected 1 row, got %d", res.RowCount)
	}
}

func TestExecutorDropIndex(t *testing.T) {
	ex := newTestExecutor(t)
	ctx := context.Background()

	_, _ = ex.Execute(ctx, "CREATE TABLE idx_t (id INT PRIMARY KEY, v TEXT)")
	_, _ = ex.Execute(ctx, "CREATE INDEX idx1 ON idx_t (v)")

	res, err := ex.Execute(ctx, "DROP INDEX idx1")
	if err != nil {
		t.Fatal(err)
	}
	if res.Message == "" {
		t.Fatal("expected message from DROP INDEX")
	}
}

// --- Transaction tests ---

func TestExecutorBeginCommit(t *testing.T) {
	ex := newTestExecutor(t)
	ctx := context.Background()

	_, _ = ex.Execute(ctx, "CREATE TABLE tx_t (id INT PRIMARY KEY, val TEXT)")

	res, err := ex.Execute(ctx, "BEGIN")
	if err != nil {
		t.Fatal(err)
	}
	if res.Message != "BEGIN" {
		t.Fatalf("expected 'BEGIN', got %q", res.Message)
	}

	_, _ = ex.Execute(ctx, "INSERT INTO tx_t (id, val) VALUES (1, 'hello')")

	res, err = ex.Execute(ctx, "COMMIT")
	if err != nil {
		t.Fatal(err)
	}
	if res.Message != "COMMIT" {
		t.Fatalf("expected 'COMMIT', got %q", res.Message)
	}

	// Data should be there
	res, err = ex.Execute(ctx, "SELECT * FROM tx_t")
	if err != nil {
		t.Fatal(err)
	}
	if res.RowCount != 1 {
		t.Fatalf("expected 1 row after commit, got %d", res.RowCount)
	}
}

func TestExecutorRollback(t *testing.T) {
	ex := newTestExecutor(t)
	ctx := context.Background()

	_, _ = ex.Execute(ctx, "CREATE TABLE tx_r (id INT PRIMARY KEY, val TEXT)")
	_, _ = ex.Execute(ctx, "INSERT INTO tx_r (id, val) VALUES (1, 'before')")
	_, _ = ex.Execute(ctx, "COMMIT")

	_, _ = ex.Execute(ctx, "BEGIN")
	_, _ = ex.Execute(ctx, "INSERT INTO tx_r (id, val) VALUES (2, 'during')")

	res, err := ex.Execute(ctx, "ROLLBACK")
	if err != nil {
		t.Fatal(err)
	}
	if res.Message != "ROLLBACK" {
		t.Fatalf("expected 'ROLLBACK', got %q", res.Message)
	}

	// Only the first row should exist
	res, err = ex.Execute(ctx, "SELECT COUNT(*) FROM tx_r")
	if err != nil {
		t.Fatal(err)
	}
	count := res.Rows[0][0].(int64)
	if count != 1 {
		t.Fatalf("expected 1 row after rollback, got %d", count)
	}
}

// --- Parser tests for new syntax ---

func TestParseSelectGroupBy(t *testing.T) {
	stmt, err := ParseSQL("SELECT category, SUM(amount) FROM orders GROUP BY category HAVING SUM(amount) > 100")
	if err != nil {
		t.Fatal(err)
	}
	sel := stmt.(*SelectStmt)
	if len(sel.GroupBy) != 1 || sel.GroupBy[0] != "category" {
		t.Fatalf("expected GROUP BY category, got %v", sel.GroupBy)
	}
	if sel.Having == nil {
		t.Fatal("expected HAVING clause")
	}
}

func TestParseSelectJoin(t *testing.T) {
	stmt, err := ParseSQL("SELECT a.id, b.name FROM t1 INNER JOIN t2 ON a.id = b.id")
	if err != nil {
		t.Fatal(err)
	}
	sel := stmt.(*SelectStmt)
	if sel.From != "t1" {
		t.Fatalf("expected FROM t1, got %q", sel.From)
	}
	if len(sel.Joins) != 1 {
		t.Fatalf("expected 1 JOIN, got %d", len(sel.Joins))
	}
	if sel.Joins[0].Type != "INNER" || sel.Joins[0].Table != "t2" {
		t.Fatalf("unexpected JOIN: %+v", sel.Joins[0])
	}
}

func TestParseSelectLeftJoin(t *testing.T) {
	stmt, err := ParseSQL("SELECT * FROM a LEFT JOIN b ON a.id = b.id")
	if err != nil {
		t.Fatal(err)
	}
	sel := stmt.(*SelectStmt)
	if len(sel.Joins) != 1 || sel.Joins[0].Type != "LEFT" {
		t.Fatalf("expected LEFT JOIN, got %+v", sel.Joins)
	}
}

func TestParseCreateIndex(t *testing.T) {
	stmt, err := ParseSQL("CREATE INDEX idx_name ON users (name)")
	if err != nil {
		t.Fatal(err)
	}
	ci, ok := stmt.(*CreateIndexStmt)
	if !ok {
		t.Fatalf("expected CreateIndexStmt, got %T", stmt)
	}
	if ci.Name != "idx_name" || ci.Table != "users" || len(ci.Columns) != 1 {
		t.Fatalf("unexpected: %+v", ci)
	}
}

func TestParseDropIndex(t *testing.T) {
	stmt, err := ParseSQL("DROP INDEX IF EXISTS idx_name")
	if err != nil {
		t.Fatal(err)
	}
	di, ok := stmt.(*DropIndexStmt)
	if !ok {
		t.Fatalf("expected DropIndexStmt, got %T", stmt)
	}
	if di.Name != "idx_name" || !di.IfExists {
		t.Fatalf("unexpected: %+v", di)
	}
}

func TestParseBeginCommitRollback(t *testing.T) {
	stmt, err := ParseSQL("BEGIN")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := stmt.(*BeginStmt); !ok {
		t.Fatalf("expected BeginStmt, got %T", stmt)
	}

	stmt, err = ParseSQL("COMMIT")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := stmt.(*CommitStmt); !ok {
		t.Fatalf("expected CommitStmt, got %T", stmt)
	}

	stmt, err = ParseSQL("ROLLBACK")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := stmt.(*RollbackStmt); !ok {
		t.Fatalf("expected RollbackStmt, got %T", stmt)
	}
}

func TestParseDistinct(t *testing.T) {
	stmt, err := ParseSQL("SELECT DISTINCT name FROM users")
	if err != nil {
		t.Fatal(err)
	}
	sel := stmt.(*SelectStmt)
	if !sel.Distinct {
		t.Fatal("expected DISTINCT")
	}
}

func TestParseIsNull(t *testing.T) {
	stmt, err := ParseSQL("SELECT * FROM t WHERE x IS NULL")
	if err != nil {
		t.Fatal(err)
	}
	sel := stmt.(*SelectStmt)
	isNull, ok := sel.Where.(*IsNullExpr)
	if !ok {
		t.Fatalf("expected IsNullExpr, got %T", sel.Where)
	}
	if isNull.Not {
		t.Fatal("expected IS NULL, not IS NOT NULL")
	}
}

func TestParseIsNotNull(t *testing.T) {
	stmt, err := ParseSQL("SELECT * FROM t WHERE x IS NOT NULL")
	if err != nil {
		t.Fatal(err)
	}
	sel := stmt.(*SelectStmt)
	isNull, ok := sel.Where.(*IsNullExpr)
	if !ok {
		t.Fatalf("expected IsNullExpr, got %T", sel.Where)
	}
	if !isNull.Not {
		t.Fatal("expected IS NOT NULL")
	}
}

func TestParseIn(t *testing.T) {
	stmt, err := ParseSQL("SELECT * FROM t WHERE id IN (1, 2, 3)")
	if err != nil {
		t.Fatal(err)
	}
	sel := stmt.(*SelectStmt)
	inExpr, ok := sel.Where.(*InExpr)
	if !ok {
		t.Fatalf("expected InExpr, got %T", sel.Where)
	}
	if len(inExpr.Values) != 3 {
		t.Fatalf("expected 3 values in IN, got %d", len(inExpr.Values))
	}
	if inExpr.Not {
		t.Fatal("expected IN, not NOT IN")
	}
}

func TestParseSumAvgMinMax(t *testing.T) {
	for _, fn := range []string{"SUM", "AVG", "MIN", "MAX"} {
		stmt, err := ParseSQL(fmt.Sprintf("SELECT %s(amount) FROM orders", fn))
		if err != nil {
			t.Fatalf("%s parse: %v", fn, err)
		}
		sel := stmt.(*SelectStmt)
		fe, ok := sel.Columns[0].Expr.(*FuncExpr)
		if !ok {
			t.Fatalf("%s: expected FuncExpr, got %T", fn, sel.Columns[0].Expr)
		}
		if fe.Name != strings.ToLower(fn) {
			t.Fatalf("expected name %q, got %q", strings.ToLower(fn), fe.Name)
		}
	}
}

func TestLexerNewKeywords(t *testing.T) {
	input := "SUM AVG MIN MAX GROUP HAVING JOIN ON LEFT INNER BEGIN ROLLBACK"
	lex := NewLexer(input)
	tokens, err := lex.Tokenize()
	if err != nil {
		t.Fatal(err)
	}

	expected := []TokenType{TokSum, TokAvg, TokMin, TokMax, TokGroup, TokHaving,
		TokJoin, TokOn, TokLeft, TokInner, TokBegin, TokRollback, TokEOF}
	if len(tokens) != len(expected) {
		t.Fatalf("expected %d tokens, got %d", len(expected), len(tokens))
	}
	for i, e := range expected {
		if tokens[i].Type != e {
			t.Fatalf("token[%d]: expected %v, got %v (%q)", i, e, tokens[i].Type, tokens[i].Value)
		}
	}
}

// helper
func toTestFloat(v interface{}) float64 {
	switch val := v.(type) {
	case float64:
		return val
	case int64:
		return float64(val)
	case int:
		return float64(val)
	}
	return 0
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && searchString(s, substr)
}

func searchString(s, sub string) bool {
	for i := 0; i <= len(s)-len(sub); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

