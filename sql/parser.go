// Package sql - parser.go parses tokens into an AST.
package sql

import (
	"fmt"
	"strconv"
	"strings"
)

// --- AST Node Types ---

// Statement is the interface for all SQL statements.
type Statement interface {
	statementNode()
}

// CreateTableStmt represents CREATE TABLE.
type CreateTableStmt struct {
	Name        string
	Columns     []ColumnDef
	IfNotExists bool
}

func (s *CreateTableStmt) statementNode() {}

// ColumnDef defines a column in CREATE TABLE.
type ColumnDef struct {
	Name     string
	Type     string
	PK       bool
	Nullable bool
}

// DropTableStmt represents DROP TABLE.
type DropTableStmt struct {
	Name     string
	IfExists bool
}

func (s *DropTableStmt) statementNode() {}

// CreateIndexStmt represents CREATE INDEX.
type CreateIndexStmt struct {
	Name        string
	Table       string
	Columns     []string
	IfNotExists bool
	Unique      bool
}

func (s *CreateIndexStmt) statementNode() {}

// DropIndexStmt represents DROP INDEX.
type DropIndexStmt struct {
	Name     string
	IfExists bool
}

func (s *DropIndexStmt) statementNode() {}

// InsertStmt represents INSERT INTO.
type InsertStmt struct {
	Table   string
	Columns []string
	Values  [][]Expr
}

func (s *InsertStmt) statementNode() {}

// SelectStmt represents SELECT.
type SelectStmt struct {
	Distinct bool
	Columns  []SelectColumn
	From     string
	Joins    []JoinClause
	Where    Expr
	GroupBy  []string
	Having   Expr
	OrderBy  []OrderByClause
	Limit    int
	Offset   int
	HasLimit bool
}

func (s *SelectStmt) statementNode() {}

// JoinClause represents a JOIN in a SELECT.
type JoinClause struct {
	Type  string // "INNER", "LEFT", "RIGHT"
	Table string
	Alias string
	On    Expr
}

// SelectColumn represents a column in SELECT.
type SelectColumn struct {
	Expr  Expr
	Alias string
	Star  bool
}

// OrderByClause represents ORDER BY column.
type OrderByClause struct {
	Expr Expr
	Desc bool
	// Kept for backward compat but Expr is preferred
	Column string
}

// DeleteStmt represents DELETE FROM.
type DeleteStmt struct {
	Table string
	Where Expr
}

func (s *DeleteStmt) statementNode() {}

// UpdateStmt represents UPDATE.
type UpdateStmt struct {
	Table string
	Set   []SetClause
	Where Expr
}

func (s *UpdateStmt) statementNode() {}

// SetClause represents SET column = value.
type SetClause struct {
	Column string
	Value  Expr
}

// BeginStmt represents BEGIN TRANSACTION.
type BeginStmt struct{}

func (s *BeginStmt) statementNode() {}

// CommitStmt represents COMMIT.
type CommitStmt struct{}

func (s *CommitStmt) statementNode() {}

// RollbackStmt represents ROLLBACK.
type RollbackStmt struct{}

func (s *RollbackStmt) statementNode() {}

// --- Expression Types ---

// Expr is the interface for all expressions.
type Expr interface {
	exprNode()
	String() string
}

// LiteralExpr is a literal value.
type LiteralExpr struct {
	Value interface{} // string, int64, float64, bool, nil
}

func (e *LiteralExpr) exprNode() {}
func (e *LiteralExpr) String() string {
	if e.Value == nil {
		return "NULL"
	}
	return fmt.Sprintf("%v", e.Value)
}

// ColumnRef references a column by name, optionally qualified by table.
type ColumnRef struct {
	Table string // optional table/alias qualifier
	Name  string
}

func (e *ColumnRef) exprNode() {}
func (e *ColumnRef) String() string {
	if e.Table != "" {
		return e.Table + "." + e.Name
	}
	return e.Name
}

// BinaryExpr is a binary operation (comparison, arithmetic, logical).
type BinaryExpr struct {
	Left  Expr
	Op    string
	Right Expr
}

func (e *BinaryExpr) exprNode() {}
func (e *BinaryExpr) String() string {
	return fmt.Sprintf("(%s %s %s)", e.Left.String(), e.Op, e.Right.String())
}

// UnaryExpr is a unary operation (NOT, -).
type UnaryExpr struct {
	Op   string
	Expr Expr
}

func (e *UnaryExpr) exprNode() {}
func (e *UnaryExpr) String() string {
	return fmt.Sprintf("(%s %s)", e.Op, e.Expr.String())
}

// FuncExpr represents a function call (COUNT, SUM, AVG, MIN, MAX).
type FuncExpr struct {
	Name     string
	Args     []Expr
	Star     bool // COUNT(*)
	Distinct bool // COUNT(DISTINCT col)
}

func (e *FuncExpr) exprNode() {}
func (e *FuncExpr) String() string {
	if e.Star {
		return fmt.Sprintf("%s(*)", e.Name)
	}
	var args []string
	for _, a := range e.Args {
		args = append(args, a.String())
	}
	prefix := ""
	if e.Distinct {
		prefix = "DISTINCT "
	}
	return fmt.Sprintf("%s(%s%s)", e.Name, prefix, strings.Join(args, ", "))
}

// InExpr represents col IN (val1, val2, ...).
type InExpr struct {
	Expr   Expr
	Values []Expr
	Not    bool
}

func (e *InExpr) exprNode() {}
func (e *InExpr) String() string {
	not := ""
	if e.Not {
		not = "NOT "
	}
	var vals []string
	for _, v := range e.Values {
		vals = append(vals, v.String())
	}
	return fmt.Sprintf("(%s %sIN (%s))", e.Expr.String(), not, strings.Join(vals, ", "))
}

// IsNullExpr represents col IS NULL / col IS NOT NULL.
type IsNullExpr struct {
	Expr Expr
	Not  bool // IS NOT NULL
}

func (e *IsNullExpr) exprNode() {}
func (e *IsNullExpr) String() string {
	if e.Not {
		return fmt.Sprintf("(%s IS NOT NULL)", e.Expr.String())
	}
	return fmt.Sprintf("(%s IS NULL)", e.Expr.String())
}

// --- Parser ---

// Parser parses SQL tokens into statements.
type Parser struct {
	tokens []Token
	pos    int
}

// NewParser creates a new parser.
func NewParser(tokens []Token) *Parser {
	return &Parser{tokens: tokens}
}

// Parse parses a single statement.
func (p *Parser) Parse() (Statement, error) {
	tok := p.peek()
	switch tok.Type {
	case TokSelect:
		return p.parseSelect()
	case TokInsert:
		return p.parseInsert()
	case TokCreate:
		return p.parseCreate()
	case TokDrop:
		return p.parseDrop()
	case TokDelete:
		return p.parseDelete()
	case TokUpdate:
		return p.parseUpdate()
	case TokBegin:
		p.advance()
		return &BeginStmt{}, nil
	case TokRollback:
		p.advance()
		return &RollbackStmt{}, nil
	case TokIdent:
		if strings.EqualFold(tok.Value, "commit") {
			p.advance()
			return &CommitStmt{}, nil
		}
		return nil, fmt.Errorf("unexpected token %v at position %d", tok.Value, tok.Pos)
	default:
		return nil, fmt.Errorf("unexpected token %v at position %d", tok.Value, tok.Pos)
	}
}

func (p *Parser) peek() Token {
	if p.pos >= len(p.tokens) {
		return Token{Type: TokEOF}
	}
	return p.tokens[p.pos]
}

func (p *Parser) advance() Token {
	tok := p.peek()
	p.pos++
	return tok
}

func (p *Parser) expect(t TokenType) (Token, error) {
	tok := p.advance()
	if tok.Type != t {
		return tok, fmt.Errorf("expected %v, got %v (%q) at position %d", t, tok.Type, tok.Value, tok.Pos)
	}
	return tok, nil
}

func (p *Parser) match(types ...TokenType) bool {
	tok := p.peek()
	for _, t := range types {
		if tok.Type == t {
			return true
		}
	}
	return false
}

func (p *Parser) matchIdent(name string) bool {
	tok := p.peek()
	return tok.Type == TokIdent && strings.EqualFold(tok.Value, name)
}

// parseCreate handles CREATE TABLE and CREATE INDEX.
func (p *Parser) parseCreate() (Statement, error) {
	p.advance() // consume CREATE

	// CREATE INDEX
	if p.match(TokIndex) || p.matchIdent("unique") {
		return p.parseCreateIndex()
	}

	if _, err := p.expect(TokTable); err != nil {
		return nil, err
	}

	stmt := &CreateTableStmt{}

	// IF NOT EXISTS
	if p.match(TokIf) {
		p.advance() // IF
		if _, err := p.expect(TokNot); err != nil {
			return nil, err
		}
		if _, err := p.expect(TokExists); err != nil {
			return nil, err
		}
		stmt.IfNotExists = true
	}

	name, err := p.expect(TokIdent)
	if err != nil {
		return nil, err
	}
	stmt.Name = name.Value

	if _, err := p.expect(TokLParen); err != nil {
		return nil, err
	}

	// Parse columns
	for {
		col, err := p.parseColumnDef()
		if err != nil {
			return nil, err
		}
		stmt.Columns = append(stmt.Columns, col)

		if !p.match(TokComma) {
			break
		}
		p.advance() // consume comma
	}

	if _, err := p.expect(TokRParen); err != nil {
		return nil, err
	}

	return stmt, nil
}

func (p *Parser) parseCreateIndex() (*CreateIndexStmt, error) {
	stmt := &CreateIndexStmt{}

	// Handle UNIQUE keyword
	if p.matchIdent("unique") {
		p.advance()
		stmt.Unique = true
	}

	if p.match(TokIndex) {
		p.advance() // consume INDEX
	}

	// IF NOT EXISTS
	if p.match(TokIf) {
		p.advance()
		if _, err := p.expect(TokNot); err != nil {
			return nil, err
		}
		if _, err := p.expect(TokExists); err != nil {
			return nil, err
		}
		stmt.IfNotExists = true
	}

	name, err := p.expect(TokIdent)
	if err != nil {
		return nil, err
	}
	stmt.Name = name.Value

	if _, err := p.expect(TokOn); err != nil {
		return nil, err
	}

	table, err := p.expect(TokIdent)
	if err != nil {
		return nil, err
	}
	stmt.Table = table.Value

	if _, err := p.expect(TokLParen); err != nil {
		return nil, err
	}

	for {
		col, err := p.expect(TokIdent)
		if err != nil {
			return nil, err
		}
		stmt.Columns = append(stmt.Columns, col.Value)
		if !p.match(TokComma) {
			break
		}
		p.advance()
	}

	if _, err := p.expect(TokRParen); err != nil {
		return nil, err
	}

	return stmt, nil
}

func (p *Parser) parseColumnDef() (ColumnDef, error) {
	name, err := p.expect(TokIdent)
	if err != nil {
		return ColumnDef{}, err
	}

	col := ColumnDef{
		Name:     name.Value,
		Nullable: true,
	}

	// Type
	tok := p.advance()
	switch tok.Type {
	case TokInt:
		col.Type = "int"
	case TokText:
		col.Type = "text"
	case TokFloat:
		col.Type = "float"
	case TokBlob:
		col.Type = "blob"
	default:
		return col, fmt.Errorf("expected column type, got %q at position %d", tok.Value, tok.Pos)
	}

	// Optional: VARCHAR(n) - consume but ignore the length
	if col.Type == "text" && p.match(TokLParen) {
		p.advance() // (
		p.advance() // number
		p.advance() // )
	}

	// Constraints
	for p.match(TokPrimary, TokNot, TokNull) {
		tok := p.peek()
		switch tok.Type {
		case TokPrimary:
			p.advance() // PRIMARY
			// Accept KEY keyword or "key" identifier
			next := p.peek()
			if next.Type == TokKey || (next.Type == TokIdent && strings.EqualFold(next.Value, "key")) {
				p.advance()
			} else {
				return col, fmt.Errorf("expected KEY after PRIMARY, got %q", next.Value)
			}
			col.PK = true
			col.Nullable = false
		case TokNot:
			p.advance() // NOT
			if _, err := p.expect(TokNull); err != nil {
				return col, err
			}
			col.Nullable = false
		case TokNull:
			p.advance() // NULL
			col.Nullable = true
		}
	}

	return col, nil
}

// parseDrop handles DROP TABLE and DROP INDEX.
func (p *Parser) parseDrop() (Statement, error) {
	p.advance() // consume DROP

	if p.match(TokIndex) {
		return p.parseDropIndex()
	}

	if _, err := p.expect(TokTable); err != nil {
		return nil, err
	}

	stmt := &DropTableStmt{}

	if p.match(TokIf) {
		p.advance()
		if _, err := p.expect(TokExists); err != nil {
			return nil, err
		}
		stmt.IfExists = true
	}

	name, err := p.expect(TokIdent)
	if err != nil {
		return nil, err
	}
	stmt.Name = name.Value

	return stmt, nil
}

func (p *Parser) parseDropIndex() (*DropIndexStmt, error) {
	p.advance() // consume INDEX
	stmt := &DropIndexStmt{}

	if p.match(TokIf) {
		p.advance()
		if _, err := p.expect(TokExists); err != nil {
			return nil, err
		}
		stmt.IfExists = true
	}

	name, err := p.expect(TokIdent)
	if err != nil {
		return nil, err
	}
	stmt.Name = name.Value

	return stmt, nil
}

// parseInsert handles INSERT INTO table (cols) VALUES (...).
func (p *Parser) parseInsert() (*InsertStmt, error) {
	p.advance() // consume INSERT
	if _, err := p.expect(TokInto); err != nil {
		return nil, err
	}

	name, err := p.expect(TokIdent)
	if err != nil {
		return nil, err
	}

	stmt := &InsertStmt{Table: name.Value}

	// Optional column list
	if p.match(TokLParen) {
		p.advance() // (
		for {
			col, err := p.expect(TokIdent)
			if err != nil {
				return nil, err
			}
			stmt.Columns = append(stmt.Columns, col.Value)
			if !p.match(TokComma) {
				break
			}
			p.advance()
		}
		if _, err := p.expect(TokRParen); err != nil {
			return nil, err
		}
	}

	if _, err := p.expect(TokValues); err != nil {
		return nil, err
	}

	// Parse value lists
	for {
		if _, err := p.expect(TokLParen); err != nil {
			return nil, err
		}

		var values []Expr
		for {
			expr, err := p.parseExpr()
			if err != nil {
				return nil, err
			}
			values = append(values, expr)
			if !p.match(TokComma) {
				break
			}
			p.advance()
		}

		if _, err := p.expect(TokRParen); err != nil {
			return nil, err
		}
		stmt.Values = append(stmt.Values, values)

		if !p.match(TokComma) {
			break
		}
		p.advance()
	}

	return stmt, nil
}

// parseSelect handles SELECT.
func (p *Parser) parseSelect() (*SelectStmt, error) {
	p.advance() // consume SELECT
	stmt := &SelectStmt{Limit: -1}

	// DISTINCT
	if p.match(TokDistinct) {
		p.advance()
		stmt.Distinct = true
	}

	// Parse select columns
	for {
		col, err := p.parseSelectColumn()
		if err != nil {
			return nil, err
		}
		stmt.Columns = append(stmt.Columns, col)
		if !p.match(TokComma) {
			break
		}
		p.advance()
	}

	// FROM
	if p.match(TokFrom) {
		p.advance()
		name, err := p.expect(TokIdent)
		if err != nil {
			return nil, err
		}
		stmt.From = name.Value
	}

	// JOINs
	for p.match(TokJoin, TokLeft, TokInner, TokRight) {
		join, err := p.parseJoin()
		if err != nil {
			return nil, err
		}
		stmt.Joins = append(stmt.Joins, join)
	}

	// WHERE
	if p.match(TokWhere) {
		p.advance()
		expr, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		stmt.Where = expr
	}

	// GROUP BY
	if p.match(TokGroup) {
		p.advance()
		if _, err := p.expect(TokBy); err != nil {
			return nil, err
		}
		for {
			col, err := p.expect(TokIdent)
			if err != nil {
				return nil, err
			}
			name := col.Value
			// Support table.column qualified names
			if p.match(TokDot) {
				p.advance()
				col2 := p.advance()
				name = name + "." + col2.Value
			}
			stmt.GroupBy = append(stmt.GroupBy, name)
			if !p.match(TokComma) {
				break
			}
			p.advance()
		}
	}

	// HAVING
	if p.match(TokHaving) {
		p.advance()
		expr, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		stmt.Having = expr
	}

	// ORDER BY
	if p.match(TokOrder) {
		p.advance()
		if _, err := p.expect(TokBy); err != nil {
			return nil, err
		}
		for {
			expr, err := p.parseExpr()
			if err != nil {
				return nil, err
			}
			clause := OrderByClause{Expr: expr}
			// Also set Column for backward compat
			if ref, ok := expr.(*ColumnRef); ok {
				clause.Column = ref.Name
			}
			if p.match(TokDesc) {
				p.advance()
				clause.Desc = true
			} else if p.match(TokAsc) {
				p.advance()
			}
			stmt.OrderBy = append(stmt.OrderBy, clause)
			if !p.match(TokComma) {
				break
			}
			p.advance()
		}
	}

	// LIMIT
	if p.match(TokLimit) {
		p.advance()
		num, err := p.expect(TokNumber)
		if err != nil {
			return nil, err
		}
		n, _ := strconv.Atoi(num.Value)
		stmt.Limit = n
		stmt.HasLimit = true
	}

	// OFFSET
	if p.match(TokOffset) {
		p.advance()
		num, err := p.expect(TokNumber)
		if err != nil {
			return nil, err
		}
		n, _ := strconv.Atoi(num.Value)
		stmt.Offset = n
	}

	return stmt, nil
}

func (p *Parser) parseJoin() (JoinClause, error) {
	join := JoinClause{Type: "INNER"}

	if p.match(TokLeft) {
		p.advance()
		join.Type = "LEFT"
		if p.match(TokOuter) {
			p.advance() // consume optional OUTER
		}
	} else if p.match(TokRight) {
		p.advance()
		join.Type = "RIGHT"
		if p.match(TokOuter) {
			p.advance()
		}
	} else if p.match(TokInner) {
		p.advance()
		join.Type = "INNER"
	}

	if _, err := p.expect(TokJoin); err != nil {
		return join, err
	}

	table, err := p.expect(TokIdent)
	if err != nil {
		return join, err
	}
	join.Table = table.Value

	// Optional alias
	if p.match(TokAs) {
		p.advance()
		alias, err := p.expect(TokIdent)
		if err != nil {
			return join, err
		}
		join.Alias = alias.Value
	} else if p.match(TokIdent) && !p.match(TokOn) {
		// Implicit alias (no AS keyword)
		tok := p.peek()
		if tok.Type == TokIdent && !strings.EqualFold(tok.Value, "on") {
			join.Alias = p.advance().Value
		}
	}

	if _, err := p.expect(TokOn); err != nil {
		return join, err
	}

	on, err := p.parseExpr()
	if err != nil {
		return join, err
	}
	join.On = on

	return join, nil
}

func (p *Parser) parseSelectColumn() (SelectColumn, error) {
	if p.match(TokStar) {
		p.advance()
		return SelectColumn{Star: true}, nil
	}

	// Aggregate functions: COUNT, SUM, AVG, MIN, MAX
	if p.match(TokCount, TokSum, TokAvg, TokMin, TokMax) {
		return p.parseAggregateColumn()
	}

	expr, err := p.parseExpr()
	if err != nil {
		return SelectColumn{}, err
	}

	col := SelectColumn{Expr: expr}
	if p.match(TokAs) {
		p.advance()
		alias, err := p.expect(TokIdent)
		if err != nil {
			return SelectColumn{}, err
		}
		col.Alias = alias.Value
	}

	return col, nil
}

func (p *Parser) parseAggregateColumn() (SelectColumn, error) {
	funcTok := p.advance()
	funcName := strings.ToLower(funcTok.Value)

	if _, err := p.expect(TokLParen); err != nil {
		return SelectColumn{}, err
	}

	fe := &FuncExpr{Name: funcName}

	if p.match(TokStar) {
		p.advance()
		fe.Star = true
	} else {
		if p.match(TokDistinct) {
			p.advance()
			fe.Distinct = true
		}
		arg, err := p.parseExpr()
		if err != nil {
			return SelectColumn{}, err
		}
		fe.Args = []Expr{arg}
	}

	if _, err := p.expect(TokRParen); err != nil {
		return SelectColumn{}, err
	}

	col := SelectColumn{Expr: fe}
	if p.match(TokAs) {
		p.advance()
		alias, err := p.expect(TokIdent)
		if err != nil {
			return SelectColumn{}, err
		}
		col.Alias = alias.Value
	}

	return col, nil
}

// parseDelete handles DELETE FROM.
func (p *Parser) parseDelete() (*DeleteStmt, error) {
	p.advance() // consume DELETE
	if _, err := p.expect(TokFrom); err != nil {
		return nil, err
	}

	name, err := p.expect(TokIdent)
	if err != nil {
		return nil, err
	}

	stmt := &DeleteStmt{Table: name.Value}

	if p.match(TokWhere) {
		p.advance()
		expr, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		stmt.Where = expr
	}

	return stmt, nil
}

// parseUpdate handles UPDATE.
func (p *Parser) parseUpdate() (*UpdateStmt, error) {
	p.advance() // consume UPDATE

	name, err := p.expect(TokIdent)
	if err != nil {
		return nil, err
	}

	stmt := &UpdateStmt{Table: name.Value}

	if _, err := p.expect(TokSet); err != nil {
		return nil, err
	}

	for {
		col, err := p.expect(TokIdent)
		if err != nil {
			return nil, err
		}
		if _, err := p.expect(TokEq); err != nil {
			return nil, err
		}
		val, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		stmt.Set = append(stmt.Set, SetClause{Column: col.Value, Value: val})
		if !p.match(TokComma) {
			break
		}
		p.advance()
	}

	if p.match(TokWhere) {
		p.advance()
		expr, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		stmt.Where = expr
	}

	return stmt, nil
}

// --- Expression parsing (precedence climbing) ---

func (p *Parser) parseExpr() (Expr, error) {
	return p.parseOr()
}

func (p *Parser) parseOr() (Expr, error) {
	left, err := p.parseAnd()
	if err != nil {
		return nil, err
	}
	for p.match(TokOr) {
		p.advance()
		right, err := p.parseAnd()
		if err != nil {
			return nil, err
		}
		left = &BinaryExpr{Left: left, Op: "OR", Right: right}
	}
	return left, nil
}

func (p *Parser) parseAnd() (Expr, error) {
	left, err := p.parseNot()
	if err != nil {
		return nil, err
	}
	for p.match(TokAnd) {
		p.advance()
		right, err := p.parseNot()
		if err != nil {
			return nil, err
		}
		left = &BinaryExpr{Left: left, Op: "AND", Right: right}
	}
	return left, nil
}

func (p *Parser) parseNot() (Expr, error) {
	if p.match(TokNot) {
		p.advance()
		expr, err := p.parseNot()
		if err != nil {
			return nil, err
		}
		return &UnaryExpr{Op: "NOT", Expr: expr}, nil
	}
	return p.parseComparison()
}

func (p *Parser) parseComparison() (Expr, error) {
	left, err := p.parseAddSub()
	if err != nil {
		return nil, err
	}

	// IS NULL / IS NOT NULL
	if p.match(TokIs) {
		p.advance()
		not := false
		if p.match(TokNot) {
			p.advance()
			not = true
		}
		if _, err := p.expect(TokNull); err != nil {
			return nil, err
		}
		return &IsNullExpr{Expr: left, Not: not}, nil
	}

	// NOT IN / IN
	if p.match(TokNot) {
		saved := p.pos
		p.advance()
		if p.match(TokIn) {
			p.advance()
			return p.parseInList(left, true)
		}
		// Not an IN expression, restore
		p.pos = saved
	}

	if p.match(TokIn) {
		p.advance()
		return p.parseInList(left, false)
	}

	if p.match(TokEq, TokNeq, TokLt, TokGt, TokLte, TokGte, TokLike) {
		tok := p.advance()
		right, err := p.parseAddSub()
		if err != nil {
			return nil, err
		}
		return &BinaryExpr{Left: left, Op: strings.ToUpper(tok.Value), Right: right}, nil
	}

	return left, nil
}

func (p *Parser) parseInList(expr Expr, not bool) (Expr, error) {
	if _, err := p.expect(TokLParen); err != nil {
		return nil, err
	}
	var values []Expr
	for {
		val, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		values = append(values, val)
		if !p.match(TokComma) {
			break
		}
		p.advance()
	}
	if _, err := p.expect(TokRParen); err != nil {
		return nil, err
	}
	return &InExpr{Expr: expr, Values: values, Not: not}, nil
}

func (p *Parser) parseAddSub() (Expr, error) {
	left, err := p.parseMulDiv()
	if err != nil {
		return nil, err
	}
	for p.match(TokPlus, TokMinus) {
		tok := p.advance()
		right, err := p.parseMulDiv()
		if err != nil {
			return nil, err
		}
		left = &BinaryExpr{Left: left, Op: tok.Value, Right: right}
	}
	return left, nil
}

func (p *Parser) parseMulDiv() (Expr, error) {
	left, err := p.parsePrimary()
	if err != nil {
		return nil, err
	}
	for p.match(TokStar, TokSlash) {
		tok := p.advance()
		right, err := p.parsePrimary()
		if err != nil {
			return nil, err
		}
		left = &BinaryExpr{Left: left, Op: tok.Value, Right: right}
	}
	return left, nil
}

func (p *Parser) parsePrimary() (Expr, error) {
	tok := p.peek()

	switch tok.Type {
	case TokNumber:
		p.advance()
		if strings.Contains(tok.Value, ".") {
			f, _ := strconv.ParseFloat(tok.Value, 64)
			return &LiteralExpr{Value: f}, nil
		}
		n, _ := strconv.ParseInt(tok.Value, 10, 64)
		return &LiteralExpr{Value: n}, nil

	case TokString:
		p.advance()
		return &LiteralExpr{Value: tok.Value}, nil

	case TokBoolTrue:
		p.advance()
		return &LiteralExpr{Value: true}, nil

	case TokBoolFalse:
		p.advance()
		return &LiteralExpr{Value: false}, nil

	case TokNull:
		p.advance()
		return &LiteralExpr{Value: nil}, nil

	case TokCount, TokSum, TokAvg, TokMin, TokMax:
		// Aggregate in expression context (e.g., HAVING SUM(x) > 10)
		funcTok := p.advance()
		funcName := strings.ToLower(funcTok.Value)
		if _, err := p.expect(TokLParen); err != nil {
			return nil, err
		}
		fe := &FuncExpr{Name: funcName}
		if p.match(TokStar) {
			p.advance()
			fe.Star = true
		} else {
			if p.match(TokDistinct) {
				p.advance()
				fe.Distinct = true
			}
			arg, err := p.parseExpr()
			if err != nil {
				return nil, err
			}
			fe.Args = []Expr{arg}
		}
		if _, err := p.expect(TokRParen); err != nil {
			return nil, err
		}
		return fe, nil

	case TokIdent:
		p.advance()
		// Check for table.column qualification
		if p.match(TokDot) {
			p.advance() // consume .
			col := p.advance()
			if col.Type == TokIdent || col.Type == TokStar {
				if col.Type == TokStar {
					return &ColumnRef{Table: tok.Value, Name: "*"}, nil
				}
				return &ColumnRef{Table: tok.Value, Name: col.Value}, nil
			}
			return nil, fmt.Errorf("expected column name after '.', got %q", col.Value)
		}
		return &ColumnRef{Name: tok.Value}, nil

	case TokLParen:
		p.advance()
		expr, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		if _, err := p.expect(TokRParen); err != nil {
			return nil, err
		}
		return expr, nil

	case TokMinus:
		p.advance()
		expr, err := p.parsePrimary()
		if err != nil {
			return nil, err
		}
		return &UnaryExpr{Op: "-", Expr: expr}, nil
	}

	return nil, fmt.Errorf("unexpected token %v (%q) at position %d", tok.Type, tok.Value, tok.Pos)
}

// ParseSQL is a convenience function that lexes and parses a SQL string.
func ParseSQL(input string) (Statement, error) {
	lexer := NewLexer(input)
	tokens, err := lexer.Tokenize()
	if err != nil {
		return nil, fmt.Errorf("lexer error: %w", err)
	}
	parser := NewParser(tokens)
	return parser.Parse()
}
