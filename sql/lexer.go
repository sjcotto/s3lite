// Package sql implements a simple SQL parser and executor for s3lite.
// Supports: CREATE TABLE, INSERT, SELECT (with WHERE), DELETE, DROP TABLE,
// and utility commands like .tables, .stats, .bench.
package sql

import (
	"fmt"
	"strings"
	"unicode"
)

// TokenType represents the type of a SQL token.
type TokenType int

const (
	// Keywords
	TokSelect TokenType = iota
	TokInsert
	TokInto
	TokValues
	TokFrom
	TokWhere
	TokCreate
	TokDrop
	TokTable
	TokAnd
	TokOr
	TokNot
	TokDelete
	TokUpdate
	TokSet
	TokOrder
	TokBy
	TokAsc
	TokDesc
	TokLimit
	TokOffset
	TokInt
	TokText
	TokFloat
	TokBlob
	TokPrimary
	TokKey
	TokNull
	TokNotNull
	TokCount
	TokAs
	TokIf
	TokExists
	TokSum
	TokAvg
	TokMin
	TokMax
	TokGroup
	TokHaving
	TokJoin
	TokOn
	TokLeft
	TokInner
	TokRight
	TokOuter
	TokIndex
	TokUsing
	TokBegin
	TokRollback
	TokIs
	TokIn
	TokDistinct

	// Operators
	TokEq     // =
	TokNeq    // != or <>
	TokLt     // <
	TokGt     // >
	TokLte    // <=
	TokGte    // >=
	TokPlus   // +
	TokMinus  // -
	TokStar   // *
	TokSlash  // /
	TokComma  // ,
	TokLParen // (
	TokRParen // )
	TokSemi   // ;
	TokDot    // .

	// Literals
	TokIdent   // identifier
	TokString  // 'string'
	TokNumber  // 123, 3.14
	TokBoolTrue
	TokBoolFalse

	TokLike
	TokBetween

	TokEOF
)

// Token is a lexed SQL token.
type Token struct {
	Type    TokenType
	Value   string
	Pos     int
}

func (t Token) String() string {
	return fmt.Sprintf("%v(%q)", t.Type, t.Value)
}

var keywords = map[string]TokenType{
	"select":  TokSelect,
	"insert":  TokInsert,
	"into":    TokInto,
	"values":  TokValues,
	"from":    TokFrom,
	"where":   TokWhere,
	"create":  TokCreate,
	"drop":    TokDrop,
	"table":   TokTable,
	"and":     TokAnd,
	"or":      TokOr,
	"not":     TokNot,
	"delete":  TokDelete,
	"update":  TokUpdate,
	"set":     TokSet,
	"order":   TokOrder,
	"by":      TokBy,
	"asc":     TokAsc,
	"desc":    TokDesc,
	"limit":   TokLimit,
	"offset":  TokOffset,
	"int":     TokInt,
	"integer": TokInt,
	"text":    TokText,
	"varchar": TokText,
	"float":   TokFloat,
	"real":    TokFloat,
	"double":  TokFloat,
	"blob":    TokBlob,
	"primary": TokPrimary,
	"null":    TokNull,
	"count":   TokCount,
	"as":      TokAs,
	"true":    TokBoolTrue,
	"false":   TokBoolFalse,
	"like":    TokLike,
	"between": TokBetween,
	"if":       TokIf,
	"exists":   TokExists,
	"sum":      TokSum,
	"avg":      TokAvg,
	"min":      TokMin,
	"max":      TokMax,
	"group":    TokGroup,
	"having":   TokHaving,
	"join":     TokJoin,
	"on":       TokOn,
	"left":     TokLeft,
	"inner":    TokInner,
	"right":    TokRight,
	"outer":    TokOuter,
	"index":    TokIndex,
	"using":    TokUsing,
	"begin":    TokBegin,
	"rollback": TokRollback,
	"is":       TokIs,
	"in":       TokIn,
	"distinct": TokDistinct,
	"commit":   TokIdent, // handled as identifier to avoid conflict with .commit
}

// Lexer tokenizes SQL input.
type Lexer struct {
	input  string
	pos    int
	tokens []Token
}

// NewLexer creates a new lexer for the given input.
func NewLexer(input string) *Lexer {
	return &Lexer{input: input}
}

// Tokenize returns all tokens from the input.
func (l *Lexer) Tokenize() ([]Token, error) {
	for {
		tok, err := l.next()
		if err != nil {
			return nil, err
		}
		l.tokens = append(l.tokens, tok)
		if tok.Type == TokEOF {
			break
		}
	}
	return l.tokens, nil
}

func (l *Lexer) next() (Token, error) {
	l.skipWhitespace()

	if l.pos >= len(l.input) {
		return Token{Type: TokEOF, Pos: l.pos}, nil
	}

	ch := l.input[l.pos]
	startPos := l.pos

	switch {
	case ch == '\'':
		return l.readString()
	case ch == '"':
		return l.readQuotedIdent()
	case isDigit(ch):
		return l.readNumber()
	case isLetter(ch) || ch == '_':
		return l.readIdentOrKeyword()
	case ch == '=':
		l.pos++
		return Token{Type: TokEq, Value: "=", Pos: startPos}, nil
	case ch == '<':
		l.pos++
		if l.pos < len(l.input) {
			if l.input[l.pos] == '=' {
				l.pos++
				return Token{Type: TokLte, Value: "<=", Pos: startPos}, nil
			}
			if l.input[l.pos] == '>' {
				l.pos++
				return Token{Type: TokNeq, Value: "<>", Pos: startPos}, nil
			}
		}
		return Token{Type: TokLt, Value: "<", Pos: startPos}, nil
	case ch == '>':
		l.pos++
		if l.pos < len(l.input) && l.input[l.pos] == '=' {
			l.pos++
			return Token{Type: TokGte, Value: ">=", Pos: startPos}, nil
		}
		return Token{Type: TokGt, Value: ">", Pos: startPos}, nil
	case ch == '!':
		l.pos++
		if l.pos < len(l.input) && l.input[l.pos] == '=' {
			l.pos++
			return Token{Type: TokNeq, Value: "!=", Pos: startPos}, nil
		}
		return Token{}, fmt.Errorf("unexpected character '!' at position %d", startPos)
	case ch == '+':
		l.pos++
		return Token{Type: TokPlus, Value: "+", Pos: startPos}, nil
	case ch == '-':
		l.pos++
		// Check for negative number
		if l.pos < len(l.input) && isDigit(l.input[l.pos]) {
			tok, err := l.readNumber()
			if err != nil {
				return tok, err
			}
			tok.Value = "-" + tok.Value
			tok.Pos = startPos
			return tok, nil
		}
		return Token{Type: TokMinus, Value: "-", Pos: startPos}, nil
	case ch == '*':
		l.pos++
		return Token{Type: TokStar, Value: "*", Pos: startPos}, nil
	case ch == '/':
		l.pos++
		return Token{Type: TokSlash, Value: "/", Pos: startPos}, nil
	case ch == ',':
		l.pos++
		return Token{Type: TokComma, Value: ",", Pos: startPos}, nil
	case ch == '(':
		l.pos++
		return Token{Type: TokLParen, Value: "(", Pos: startPos}, nil
	case ch == ')':
		l.pos++
		return Token{Type: TokRParen, Value: ")", Pos: startPos}, nil
	case ch == ';':
		l.pos++
		return Token{Type: TokSemi, Value: ";", Pos: startPos}, nil
	case ch == '.':
		l.pos++
		return Token{Type: TokDot, Value: ".", Pos: startPos}, nil
	}

	return Token{}, fmt.Errorf("unexpected character '%c' at position %d", ch, startPos)
}

func (l *Lexer) readString() (Token, error) {
	startPos := l.pos
	l.pos++ // skip opening quote
	var sb strings.Builder

	for l.pos < len(l.input) {
		ch := l.input[l.pos]
		if ch == '\'' {
			// Check for escaped quote ('')
			if l.pos+1 < len(l.input) && l.input[l.pos+1] == '\'' {
				sb.WriteByte('\'')
				l.pos += 2
				continue
			}
			l.pos++ // skip closing quote
			return Token{Type: TokString, Value: sb.String(), Pos: startPos}, nil
		}
		sb.WriteByte(ch)
		l.pos++
	}

	return Token{}, fmt.Errorf("unterminated string at position %d", startPos)
}

func (l *Lexer) readQuotedIdent() (Token, error) {
	startPos := l.pos
	l.pos++ // skip opening quote
	var sb strings.Builder

	for l.pos < len(l.input) {
		ch := l.input[l.pos]
		if ch == '"' {
			l.pos++
			return Token{Type: TokIdent, Value: sb.String(), Pos: startPos}, nil
		}
		sb.WriteByte(ch)
		l.pos++
	}

	return Token{}, fmt.Errorf("unterminated quoted identifier at position %d", startPos)
}

func (l *Lexer) readNumber() (Token, error) {
	startPos := l.pos
	hasDot := false

	for l.pos < len(l.input) {
		ch := l.input[l.pos]
		if ch == '.' {
			if hasDot {
				break
			}
			hasDot = true
			l.pos++
			continue
		}
		if !isDigit(ch) {
			break
		}
		l.pos++
	}

	return Token{Type: TokNumber, Value: l.input[startPos:l.pos], Pos: startPos}, nil
}

func (l *Lexer) readIdentOrKeyword() (Token, error) {
	startPos := l.pos

	for l.pos < len(l.input) {
		ch := l.input[l.pos]
		if !isLetter(ch) && !isDigit(ch) && ch != '_' {
			break
		}
		l.pos++
	}

	word := l.input[startPos:l.pos]
	lower := strings.ToLower(word)

	if tokType, ok := keywords[lower]; ok {
		return Token{Type: tokType, Value: lower, Pos: startPos}, nil
	}

	return Token{Type: TokIdent, Value: word, Pos: startPos}, nil
}

func (l *Lexer) skipWhitespace() {
	for l.pos < len(l.input) && unicode.IsSpace(rune(l.input[l.pos])) {
		l.pos++
	}
}

func isDigit(ch byte) bool {
	return ch >= '0' && ch <= '9'
}

func isLetter(ch byte) bool {
	return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || ch == '_'
}
