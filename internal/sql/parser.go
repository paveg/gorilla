package sql

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/paveg/gorilla/internal/expr"
)

// TokenType represents the type of a SQL token.
type TokenType int

const (
	// Special tokens.
	EOF TokenType = iota
	ILLEGAL

	// Literals.
	IDENT  // column names, table names, function names
	INT    // integers
	FLOAT  // floating point numbers
	STRING // string literals

	// Keywords.
	SELECT
	FROM
	WHERE
	GROUP
	BY
	HAVING
	ORDER
	LIMIT
	OFFSET
	AS
	AND
	OR
	NOT
	IN
	EXISTS
	NULL
	TRUE
	FALSE
	JOIN
	INNER
	LEFT
	RIGHT
	FULL
	ON
	COUNT
	SUM
	AVG
	MIN
	MAX
	DISTINCT

	// Operators.
	EQ    // =
	NE    // != or <>
	LT    // <
	LE    // <=
	GT    // >
	GE    // >=
	PLUS  // +
	MINUS // -
	MULT  // *
	DIV   // /
	MOD   // %

	// Delimiters.
	COMMA     // ,
	SEMICOLON // ;
	LPAREN    // (
	RPAREN    // )
	DOT       // .
)

// Token represents a single SQL token.
type Token struct {
	Type     TokenType
	Literal  string
	Position int
}

// Lexer tokenizes SQL input.
type Lexer struct {
	input        string
	position     int  // current position in input (points to current char)
	readPosition int  // current reading position in input (after current char)
	ch           byte // current char under examination
}

// NewLexer creates a new lexer instance.
func NewLexer(input string) *Lexer {
	l := &Lexer{input: input}
	l.readChar()
	return l
}

// readChar reads the next character and advances position.
func (l *Lexer) readChar() {
	if l.readPosition >= len(l.input) {
		l.ch = 0 // ASCII NUL represents "EOF"
	} else {
		l.ch = l.input[l.readPosition]
	}
	l.position = l.readPosition
	l.readPosition++
}

// peekChar returns the next character without advancing position.
func (l *Lexer) peekChar() byte {
	if l.readPosition >= len(l.input) {
		return 0
	}
	return l.input[l.readPosition]
}

// NextToken scans the input and returns the next token.
func (l *Lexer) NextToken() Token {
	var tok Token

	l.skipWhitespace()

	switch l.ch {
	case '=':
		tok = Token{Type: EQ, Literal: string(l.ch), Position: l.position}
	case '!':
		if l.peekChar() == '=' {
			ch := l.ch
			l.readChar()
			tok = Token{Type: NE, Literal: string(ch) + string(l.ch), Position: l.position - 1}
		} else {
			tok = Token{Type: ILLEGAL, Literal: string(l.ch), Position: l.position}
		}
	case '<':
		if l.peekChar() == '=' {
			ch := l.ch
			l.readChar()
			tok = Token{Type: LE, Literal: string(ch) + string(l.ch), Position: l.position - 1}
		} else if l.peekChar() == '>' {
			ch := l.ch
			l.readChar()
			tok = Token{Type: NE, Literal: string(ch) + string(l.ch), Position: l.position - 1}
		} else {
			tok = Token{Type: LT, Literal: string(l.ch), Position: l.position}
		}
	case '>':
		if l.peekChar() == '=' {
			ch := l.ch
			l.readChar()
			tok = Token{Type: GE, Literal: string(ch) + string(l.ch), Position: l.position - 1}
		} else {
			tok = Token{Type: GT, Literal: string(l.ch), Position: l.position}
		}
	case '+':
		tok = Token{Type: PLUS, Literal: string(l.ch), Position: l.position}
	case '-':
		tok = Token{Type: MINUS, Literal: string(l.ch), Position: l.position}
	case '*':
		tok = Token{Type: MULT, Literal: string(l.ch), Position: l.position}
	case '/':
		tok = Token{Type: DIV, Literal: string(l.ch), Position: l.position}
	case '%':
		tok = Token{Type: MOD, Literal: string(l.ch), Position: l.position}
	case ',':
		tok = Token{Type: COMMA, Literal: string(l.ch), Position: l.position}
	case ';':
		tok = Token{Type: SEMICOLON, Literal: string(l.ch), Position: l.position}
	case '(':
		tok = Token{Type: LPAREN, Literal: string(l.ch), Position: l.position}
	case ')':
		tok = Token{Type: RPAREN, Literal: string(l.ch), Position: l.position}
	case '.':
		tok = Token{Type: DOT, Literal: string(l.ch), Position: l.position}
	case '\'', '"':
		tok.Type = STRING
		tok.Literal = l.readString()
		tok.Position = l.position
	case 0:
		tok.Literal = ""
		tok.Type = EOF
		tok.Position = l.position
	default:
		if isLetter(l.ch) {
			tok.Position = l.position
			tok.Literal = l.readIdentifier()
			tok.Type = lookupIdent(tok.Literal)
			return tok // early return to avoid readChar() call
		} else if isDigit(l.ch) {
			tok.Position = l.position
			tok.Type, tok.Literal = l.readNumber()
			return tok // early return to avoid readChar() call
		} else {
			tok = Token{Type: ILLEGAL, Literal: string(l.ch), Position: l.position}
		}
	}

	l.readChar()
	return tok
}

// skipWhitespace skips whitespace characters.
func (l *Lexer) skipWhitespace() {
	for l.ch == ' ' || l.ch == '\t' || l.ch == '\n' || l.ch == '\r' {
		l.readChar()
	}
}

// readIdentifier reads an identifier (column name, table name, keyword).
func (l *Lexer) readIdentifier() string {
	position := l.position
	for isLetter(l.ch) || isDigit(l.ch) || l.ch == '_' {
		l.readChar()
	}
	return l.input[position:l.position]
}

// readNumber reads a number (integer or float).
func (l *Lexer) readNumber() (TokenType, string) {
	position := l.position
	tokenType := INT

	for isDigit(l.ch) {
		l.readChar()
	}

	// Check for decimal point
	if l.ch == '.' && isDigit(l.peekChar()) {
		tokenType = FLOAT
		l.readChar() // consume '.'
		for isDigit(l.ch) {
			l.readChar()
		}
	}

	return tokenType, l.input[position:l.position]
}

// readString reads a string literal.
func (l *Lexer) readString() string {
	quote := l.ch
	l.readChar() // consume opening quote

	position := l.position
	for l.ch != quote && l.ch != 0 {
		l.readChar()
	}

	result := l.input[position:l.position]
	// Note: l.readChar() will be called by NextToken to consume closing quote
	return result
}

// isLetter checks if character is a letter.
func isLetter(ch byte) bool {
	return 'a' <= ch && ch <= 'z' || 'A' <= ch && ch <= 'Z' || ch == '_'
}

// isDigit checks if character is a digit.
func isDigit(ch byte) bool {
	return '0' <= ch && ch <= '9'
}

// Keywords map for identifier lookup.
var keywords = map[string]TokenType{
	"SELECT":   SELECT,
	"FROM":     FROM,
	"WHERE":    WHERE,
	"GROUP":    GROUP,
	"BY":       BY,
	"HAVING":   HAVING,
	"ORDER":    ORDER,
	"LIMIT":    LIMIT,
	"OFFSET":   OFFSET,
	"AS":       AS,
	"AND":      AND,
	"OR":       OR,
	"NOT":      NOT,
	"IN":       IN,
	"EXISTS":   EXISTS,
	"NULL":     NULL,
	"TRUE":     TRUE,
	"FALSE":    FALSE,
	"JOIN":     JOIN,
	"INNER":    INNER,
	"LEFT":     LEFT,
	"RIGHT":    RIGHT,
	"FULL":     FULL,
	"ON":       ON,
	"COUNT":    COUNT,
	"SUM":      SUM,
	"AVG":      AVG,
	"MIN":      MIN,
	"MAX":      MAX,
	"DISTINCT": DISTINCT,
}

// lookupIdent checks if identifier is a keyword.
func lookupIdent(ident string) TokenType {
	if tok, ok := keywords[strings.ToUpper(ident)]; ok {
		return tok
	}
	return IDENT
}

// Parser parses SQL tokens into AST.
type Parser struct {
	lexer *Lexer

	curToken  Token
	peekToken Token

	errors []string
}

// NewParser creates a new parser instance.
func NewParser(lexer *Lexer) *Parser {
	p := &Parser{
		lexer:  lexer,
		errors: []string{},
	}

	// Read two tokens, so curToken and peekToken are both set
	p.nextToken()
	p.nextToken()

	return p
}

// nextToken advances both curToken and peekToken.
func (p *Parser) nextToken() {
	p.curToken = p.peekToken
	p.peekToken = p.lexer.NextToken()
}

// Errors returns parse errors.
func (p *Parser) Errors() []string {
	return p.errors
}

// ParseSQL parses a SQL statement.
func (p *Parser) ParseSQL() SQLStatement {
	switch p.curToken.Type {
	case SELECT:
		return p.parseSelectStatement()
	default:
		p.addError(fmt.Sprintf("unexpected token: %s", p.curToken.Literal))
		return nil
	}
}

// parseSelectStatement parses a SELECT statement.
func (p *Parser) parseSelectStatement() *SelectStatement {
	stmt := &SelectStatement{}

	// Current token is already SELECT, no need to advance

	// Parse SELECT list
	selectList, ok := p.parseSelectList()
	if !ok {
		return nil
	}
	stmt.SelectList = selectList

	// Parse FROM clause (optional)
	if p.peekTokenIs(FROM) {
		p.nextToken()
		fromClause, ok := p.parseFromClause()
		if !ok {
			return nil
		}
		stmt.FromClause = fromClause
	}

	// Parse WHERE clause (optional)
	if p.peekTokenIs(WHERE) {
		p.nextToken()
		whereClause, ok := p.parseWhereClause()
		if !ok {
			return nil
		}
		stmt.WhereClause = whereClause
	}

	// Parse GROUP BY clause (optional)
	if p.peekTokenIs(GROUP) {
		p.nextToken()
		groupByClause, ok := p.parseGroupByClause()
		if !ok {
			return nil
		}
		stmt.GroupByClause = groupByClause
	}

	// Parse HAVING clause (optional)
	if p.peekTokenIs(HAVING) {
		p.nextToken()
		havingClause, ok := p.parseHavingClause()
		if !ok {
			return nil
		}
		stmt.HavingClause = havingClause
	}

	// Parse ORDER BY clause (optional)
	if p.peekTokenIs(ORDER) {
		p.nextToken()
		orderByClause, ok := p.parseOrderByClause()
		if !ok {
			return nil
		}
		stmt.OrderByClause = orderByClause
	}

	// Parse LIMIT clause (optional)
	if p.peekTokenIs(LIMIT) {
		p.nextToken()
		limitClause, ok := p.parseLimitClause()
		if !ok {
			return nil
		}
		stmt.LimitClause = limitClause
	} else if p.peekTokenIs(OFFSET) {
		// Parse standalone OFFSET clause (without LIMIT)
		p.nextToken()
		offsetClause, ok := p.parseStandaloneOffsetClause()
		if !ok {
			return nil
		}
		stmt.LimitClause = offsetClause
	}

	return stmt
}

// parseSelectList parses the SELECT list.
func (p *Parser) parseSelectList() ([]SelectItem, bool) {
	var items []SelectItem

	p.nextToken() // consume SELECT

	for {
		item, ok := p.parseSelectItem()
		if !ok {
			return nil, false
		}
		items = append(items, item)

		if !p.peekTokenIs(COMMA) {
			break
		}
		p.nextToken() // consume comma
		p.nextToken() // move to next item
	}

	return items, true
}

// isValidAlias checks if a token can be used as an alias.
func (p *Parser) isValidAlias() bool {
	switch p.peekToken.Type {
	case IDENT, COUNT, SUM, AVG, MIN, MAX:
		// Allow identifiers and function names as aliases
		return true
	default:
		return false
	}
}

// parseSelectItem parses a single SELECT item.
func (p *Parser) parseSelectItem() (SelectItem, bool) {
	// Handle wildcard
	if p.curTokenIs(MULT) {
		return SelectItem{IsWildcard: true}, true
	}

	// Parse expression
	expression, ok := p.parseExpression(LOWEST)
	if !ok {
		return SelectItem{}, false
	}

	// Check for alias
	var alias string
	if p.peekTokenIs(AS) {
		p.nextToken() // consume AS
		if !p.isValidAlias() {
			p.addError(fmt.Sprintf("expected alias name, got %s", p.peekToken.Literal))
			return SelectItem{}, false
		}
		p.nextToken()
		alias = p.curToken.Literal
	} else if p.isValidAlias() {
		// Implicit alias (without AS keyword)
		p.nextToken()
		alias = p.curToken.Literal
	}

	return SelectItem{
		Expression: expression,
		Alias:      alias,
	}, true
}

// Precedence constants for expression parsing.
const (
	_ int = iota
	LOWEST
	LOGICAL     // AND, OR
	EQUALS      // ==
	LESSGREATER // > or <
	SUMPREC     // + (precedence)
	PRODUCT     // *
	PREFIX      // -X or !X
	CALL        // myFunction(X)
)

// precedences maps tokens to their precedence.
var precedences = map[TokenType]int{
	AND:    LOGICAL,
	OR:     LOGICAL,
	EQ:     EQUALS,
	NE:     EQUALS,
	LT:     LESSGREATER,
	GT:     LESSGREATER,
	LE:     LESSGREATER,
	GE:     LESSGREATER,
	PLUS:   SUMPREC,
	MINUS:  SUMPREC,
	DIV:    PRODUCT,
	MULT:   PRODUCT,
	LPAREN: CALL,
}

// parseExpression parses expressions using Pratt parser.
func (p *Parser) parseExpression(precedence int) (expr.Expr, bool) {
	// Parse prefix expression
	left, ok := p.parsePrefix()
	if !ok {
		return nil, false
	}

	// Parse infix expressions
	for !p.peekTokenIs(SEMICOLON) && precedence < p.peekPrecedence() {
		left, ok = p.parseInfix(left)
		if !ok {
			return left, true
		}
	}

	return left, true
}

// parsePrefix parses prefix expressions.
func (p *Parser) parsePrefix() (expr.Expr, bool) {
	switch p.curToken.Type {
	case IDENT:
		return p.parseIdentifier()
	case INT:
		return p.parseIntegerLiteral()
	case FLOAT:
		return p.parseFloatLiteral()
	case STRING:
		return p.parseStringLiteral()
	case TRUE, FALSE:
		return p.parseBooleanLiteral()
	case NULL:
		return p.parseNullLiteral()
	case MINUS, NOT:
		return p.parseUnaryExpression()
	case LPAREN:
		return p.parseGroupedExpression()
	case COUNT, SUM, AVG, MIN, MAX:
		return p.parseFunctionCall()
	case MULT:
		// Handle * as a special literal for function arguments like COUNT(*)
		return expr.Lit(1), true
	default:
		p.addError(fmt.Sprintf("no prefix parse function for %s found", p.curToken.Literal))
		return nil, false
	}
}

// parseInfix parses infix expressions.
func (p *Parser) parseInfix(left expr.Expr) (expr.Expr, bool) {
	switch p.peekToken.Type {
	case EQ, NE, LT, LE, GT, GE, PLUS, MINUS, MULT, DIV, MOD:
		p.nextToken()
		return p.parseBinaryExpression(left)
	case AND, OR:
		p.nextToken()
		return p.parseLogicalExpression(left)
	case LPAREN:
		if _, ok := left.(*expr.ColumnExpr); ok {
			p.nextToken()
			return p.parseFunctionCallWithLeft(left.(*expr.ColumnExpr))
		}
	}
	return left, true
}

// Helper functions for token checking.
func (p *Parser) curTokenIs(t TokenType) bool {
	return p.curToken.Type == t
}

func (p *Parser) peekTokenIs(t TokenType) bool {
	return p.peekToken.Type == t
}

func (p *Parser) expectPeek(t TokenType) bool {
	if p.peekTokenIs(t) {
		p.nextToken()
		return true
	}
	p.addError(fmt.Sprintf("expected next token to be %v, got %v instead", t, p.peekToken.Type))
	return false
}

func (p *Parser) peekPrecedence() int {
	if p, ok := precedences[p.peekToken.Type]; ok {
		return p
	}
	return LOWEST
}

func (p *Parser) curPrecedence() int {
	if p, ok := precedences[p.curToken.Type]; ok {
		return p
	}
	return LOWEST
}

func (p *Parser) addError(msg string) {
	p.errors = append(p.errors, msg)
}

// Parse helper functions (implementations for remaining parse methods will be added in subsequent files).
func (p *Parser) parseIdentifier() (expr.Expr, bool) {
	// Check if this is a function call (identifier followed by LPAREN)
	if p.peekTokenIs(LPAREN) {
		return p.parseFunctionCall()
	}
	return expr.Col(p.curToken.Literal), true
}

func (p *Parser) parseIntegerLiteral() (expr.Expr, bool) {
	value, err := strconv.ParseInt(p.curToken.Literal, 10, 64)
	if err != nil {
		p.addError(fmt.Sprintf("could not parse %q as integer", p.curToken.Literal))
		return nil, false
	}

	// Note: value is stored as int64 in LiteralExpr, which is safe for all platforms
	// The validation is primarily to ensure we have a valid 64-bit signed integer
	return expr.Lit(value), true
}

func (p *Parser) parseFloatLiteral() (expr.Expr, bool) {
	value, err := strconv.ParseFloat(p.curToken.Literal, 64)
	if err != nil {
		p.addError(fmt.Sprintf("could not parse %q as float", p.curToken.Literal))
		return nil, false
	}
	return expr.Lit(value), true
}

func (p *Parser) parseStringLiteral() (expr.Expr, bool) {
	return expr.Lit(p.curToken.Literal), true
}

func (p *Parser) parseBooleanLiteral() (expr.Expr, bool) {
	return expr.Lit(p.curTokenIs(TRUE)), true
}

func (p *Parser) parseNullLiteral() (expr.Expr, bool) {
	return expr.Lit(nil), true
}

func (p *Parser) parseUnaryExpression() (expr.Expr, bool) {
	operator := p.curToken.Literal
	p.nextToken()

	_, ok := p.parseExpression(PREFIX)
	if !ok {
		return nil, false
	}

	switch operator {
	case "-":
		// Unary negation - for now, skip as it requires complex handling
		p.addError("unary negation (-) not fully supported yet")
		return nil, false
	case "NOT":
		// NOT operation - for now, skip unary NOT as it's complex to implement
		p.addError("NOT operator not fully supported yet")
		return nil, false
	default:
		p.addError(fmt.Sprintf("unknown operator: %s", operator))
		return nil, false
	}
}

func (p *Parser) parseGroupedExpression() (expr.Expr, bool) {
	p.nextToken() // consume '('

	exp, ok := p.parseExpression(LOWEST)
	if !ok {
		return nil, false
	}

	if !p.expectPeek(RPAREN) {
		return nil, false
	}

	return exp, true
}

func (p *Parser) parseBinaryExpression(left expr.Expr) (expr.Expr, bool) {
	operator := p.curToken.Literal
	precedence := p.curPrecedence()
	p.nextToken()

	right, ok := p.parseExpression(precedence)
	if !ok {
		return nil, false
	}

	// Use type assertion to call methods on concrete types
	switch operator {
	case "+":
		if col, ok := left.(*expr.ColumnExpr); ok {
			return col.Add(right), true
		} else if bin, ok := left.(*expr.BinaryExpr); ok {
			return bin.Add(right), true
		} else if fun, ok := left.(*expr.FunctionExpr); ok {
			return fun.Add(right), true
		} else if agg, ok := left.(*expr.AggregationExpr); ok {
			return agg.Add(right), true
		}
		p.addError(fmt.Sprintf("unsupported expression type for addition: %T", left))
		return nil, false
	case "-":
		if col, ok := left.(*expr.ColumnExpr); ok {
			return col.Sub(right), true
		} else if bin, ok := left.(*expr.BinaryExpr); ok {
			return bin.Sub(right), true
		} else if fun, ok := left.(*expr.FunctionExpr); ok {
			return fun.Sub(right), true
		} else if agg, ok := left.(*expr.AggregationExpr); ok {
			return agg.Sub(right), true
		}
		p.addError(fmt.Sprintf("unsupported expression type for subtraction: %T", left))
		return nil, false
	case "*":
		if col, ok := left.(*expr.ColumnExpr); ok {
			return col.Mul(right), true
		} else if bin, ok := left.(*expr.BinaryExpr); ok {
			return bin.Mul(right), true
		} else if fun, ok := left.(*expr.FunctionExpr); ok {
			return fun.Mul(right), true
		} else if agg, ok := left.(*expr.AggregationExpr); ok {
			return agg.Mul(right), true
		}
		p.addError(fmt.Sprintf("unsupported expression type for multiplication: %T", left))
		return nil, false
	case "/":
		if col, ok := left.(*expr.ColumnExpr); ok {
			return col.Div(right), true
		} else if bin, ok := left.(*expr.BinaryExpr); ok {
			return bin.Div(right), true
		} else if fun, ok := left.(*expr.FunctionExpr); ok {
			return fun.Div(right), true
		} else if agg, ok := left.(*expr.AggregationExpr); ok {
			return agg.Div(right), true
		}
		p.addError(fmt.Sprintf("unsupported expression type for division: %T", left))
		return nil, false
	case "%":
		// Modulo operation not implemented yet
		p.addError("modulo operator (%) not supported yet")
		return nil, false
	case "=":
		if col, ok := left.(*expr.ColumnExpr); ok {
			return col.Eq(right), true
		} else if fun, ok := left.(*expr.FunctionExpr); ok {
			return fun.Eq(right), true
		} else if agg, ok := left.(*expr.AggregationExpr); ok {
			return agg.Eq(right), true
		}
		p.addError(fmt.Sprintf("unsupported expression type for equality: %T", left))
		return nil, false
	case "!=", "<>":
		if col, ok := left.(*expr.ColumnExpr); ok {
			return col.Ne(right), true
		} else if fun, ok := left.(*expr.FunctionExpr); ok {
			return fun.Ne(right), true
		} else if agg, ok := left.(*expr.AggregationExpr); ok {
			return agg.Ne(right), true
		}
		p.addError(fmt.Sprintf("unsupported expression type for not equal: %T", left))
		return nil, false
	case "<":
		if col, ok := left.(*expr.ColumnExpr); ok {
			return col.Lt(right), true
		} else if fun, ok := left.(*expr.FunctionExpr); ok {
			return fun.Lt(right), true
		} else if agg, ok := left.(*expr.AggregationExpr); ok {
			return agg.Lt(right), true
		}
		p.addError(fmt.Sprintf("unsupported expression type for less than: %T", left))
		return nil, false
	case "<=":
		if col, ok := left.(*expr.ColumnExpr); ok {
			return col.Le(right), true
		} else if fun, ok := left.(*expr.FunctionExpr); ok {
			return fun.Le(right), true
		} else if agg, ok := left.(*expr.AggregationExpr); ok {
			return agg.Le(right), true
		}
		p.addError(fmt.Sprintf("unsupported expression type for less than or equal: %T", left))
		return nil, false
	case ">":
		if col, ok := left.(*expr.ColumnExpr); ok {
			return col.Gt(right), true
		} else if fun, ok := left.(*expr.FunctionExpr); ok {
			return fun.Gt(right), true
		} else if agg, ok := left.(*expr.AggregationExpr); ok {
			return agg.Gt(right), true
		}
		p.addError(fmt.Sprintf("unsupported expression type for greater than: %T", left))
		return nil, false
	case ">=":
		if col, ok := left.(*expr.ColumnExpr); ok {
			return col.Ge(right), true
		} else if fun, ok := left.(*expr.FunctionExpr); ok {
			return fun.Ge(right), true
		} else if agg, ok := left.(*expr.AggregationExpr); ok {
			return agg.Ge(right), true
		}
		p.addError(fmt.Sprintf("unsupported expression type for greater than or equal: %T", left))
		return nil, false
	default:
		p.addError(fmt.Sprintf("unknown operator: %s", operator))
		return nil, false
	}
}

func (p *Parser) parseLogicalExpression(left expr.Expr) (expr.Expr, bool) {
	operator := p.curToken.Literal
	precedence := p.curPrecedence()
	p.nextToken()

	right, ok := p.parseExpression(precedence)
	if !ok {
		return nil, false
	}

	switch strings.ToUpper(operator) {
	case "AND":
		if bin, ok := left.(*expr.BinaryExpr); ok {
			return bin.And(right), true
		} else if fun, ok := left.(*expr.FunctionExpr); ok {
			return fun.And(right), true
		} else if agg, ok := left.(*expr.AggregationExpr); ok {
			return agg.And(right), true
		}
		p.addError(fmt.Sprintf("unsupported expression type for AND operation: %T", left))
		return nil, false
	case "OR":
		if bin, ok := left.(*expr.BinaryExpr); ok {
			return bin.Or(right), true
		} else if fun, ok := left.(*expr.FunctionExpr); ok {
			return fun.Or(right), true
		} else if agg, ok := left.(*expr.AggregationExpr); ok {
			return agg.Or(right), true
		}
		p.addError(fmt.Sprintf("unsupported expression type for OR operation: %T", left))
		return nil, false
	default:
		p.addError(fmt.Sprintf("unknown logical operator: %s", operator))
		return nil, false
	}
}

func (p *Parser) parseFunctionCall() (expr.Expr, bool) {
	functionName := p.curToken.Literal

	if !p.expectPeek(LPAREN) {
		return nil, false
	}

	args, ok := p.parseExpressionList(RPAREN)
	if !ok {
		return nil, false
	}

	// Convert SQL functions to Gorilla expressions
	switch strings.ToUpper(functionName) {
	// Aggregation functions
	case CountFunction:
		if len(args) == 0 {
			return expr.Count(expr.Lit(1)), true
		}
		return expr.Count(args[0]), true
	case SumFunction:
		if len(args) != 1 {
			p.addError("SUM function requires exactly one argument")
			return nil, false
		}
		return expr.Sum(args[0]), true
	case AvgFunction:
		if len(args) != 1 {
			p.addError("AVG function requires exactly one argument")
			return nil, false
		}
		return expr.Mean(args[0]), true
	case MinFunction:
		if len(args) != 1 {
			p.addError("MIN function requires exactly one argument")
			return nil, false
		}
		return expr.Min(args[0]), true
	case MaxFunction:
		if len(args) != 1 {
			p.addError("MAX function requires exactly one argument")
			return nil, false
		}
		return expr.Max(args[0]), true

	// String functions
	case UpperFunction:
		if len(args) != 1 {
			p.addError("UPPER function requires exactly one argument")
			return nil, false
		}
		// Convert to column expression and apply Upper method
		if colExpr, ok := args[0].(*expr.ColumnExpr); ok {
			return colExpr.Upper(), true
		}
		p.addError("UPPER function requires a column argument")
		return nil, false
	case LowerFunction:
		if len(args) != 1 {
			p.addError("LOWER function requires exactly one argument")
			return nil, false
		}
		if colExpr, ok := args[0].(*expr.ColumnExpr); ok {
			return colExpr.Lower(), true
		}
		p.addError("LOWER function requires a column argument")
		return nil, false
	case LengthFunction:
		if len(args) != 1 {
			p.addError("LENGTH function requires exactly one argument")
			return nil, false
		}
		if colExpr, ok := args[0].(*expr.ColumnExpr); ok {
			return colExpr.Length(), true
		}
		p.addError("LENGTH function requires a column argument")
		return nil, false
	case TrimFunction:
		if len(args) != 1 {
			p.addError("TRIM function requires exactly one argument")
			return nil, false
		}
		if colExpr, ok := args[0].(*expr.ColumnExpr); ok {
			return colExpr.Trim(), true
		}
		p.addError("TRIM function requires a column argument")
		return nil, false

	// Math functions
	case AbsFunction:
		if len(args) != 1 {
			p.addError("ABS function requires exactly one argument")
			return nil, false
		}
		if colExpr, ok := args[0].(*expr.ColumnExpr); ok {
			return colExpr.Abs(), true
		}
		p.addError("ABS function requires a column argument")
		return nil, false
	case RoundFunction:
		if len(args) != 1 {
			p.addError("ROUND function requires exactly one argument")
			return nil, false
		}
		if colExpr, ok := args[0].(*expr.ColumnExpr); ok {
			return colExpr.Round(), true
		}
		p.addError("ROUND function requires a column argument")
		return nil, false
	case FloorFunction:
		if len(args) != 1 {
			p.addError("FLOOR function requires exactly one argument")
			return nil, false
		}
		if colExpr, ok := args[0].(*expr.ColumnExpr); ok {
			return colExpr.Floor(), true
		}
		p.addError("FLOOR function requires a column argument")
		return nil, false
	case CeilFunction:
		if len(args) != 1 {
			p.addError("CEIL function requires exactly one argument")
			return nil, false
		}
		if colExpr, ok := args[0].(*expr.ColumnExpr); ok {
			return colExpr.Ceil(), true
		}
		p.addError("CEIL function requires a column argument")
		return nil, false
	default:
		// Create generic function expression for other functions
		return expr.NewFunction(functionName, args...), true
	}
}

func (p *Parser) parseFunctionCallWithLeft(_ *expr.ColumnExpr) (expr.Expr, bool) {
	_, ok := p.parseExpressionList(RPAREN)
	if !ok {
		return nil, false
	}

	// Function calls with left context not supported yet
	p.addError("function calls with left context not supported yet")
	return nil, false
}

func (p *Parser) parseExpressionList(end TokenType) ([]expr.Expr, bool) {
	var args []expr.Expr

	if p.peekTokenIs(end) {
		p.nextToken()
		return args, true
	}

	p.nextToken()
	arg, ok := p.parseExpression(LOWEST)
	if !ok {
		return nil, false
	}
	args = append(args, arg)

	for p.peekTokenIs(COMMA) {
		p.nextToken()
		p.nextToken()
		arg, ok := p.parseExpression(LOWEST)
		if !ok {
			return nil, false
		}
		args = append(args, arg)
	}

	if !p.expectPeek(end) {
		return nil, false
	}

	return args, true
}

// Placeholder implementations for remaining clause parsers.
func (p *Parser) parseFromClause() (*FromClause, bool) {
	if !p.expectPeek(IDENT) {
		return nil, false
	}

	fromClause := &FromClause{
		TableName: p.curToken.Literal,
	}

	// Check for alias
	if p.peekTokenIs(AS) {
		p.nextToken()
		if !p.expectPeek(IDENT) {
			return nil, false
		}
		fromClause.Alias = p.curToken.Literal
	} else if p.peekTokenIs(IDENT) {
		p.nextToken()
		fromClause.Alias = p.curToken.Literal
	}

	return fromClause, true
}

func (p *Parser) parseWhereClause() (*WhereClause, bool) {
	p.nextToken()
	condition, ok := p.parseExpression(LOWEST)
	if !ok {
		return nil, false
	}

	return &WhereClause{Condition: condition}, true
}

func (p *Parser) parseGroupByClause() (*GroupByClause, bool) {
	if !p.expectPeek(BY) {
		return nil, false
	}

	var columns []expr.Expr
	p.nextToken()

	for {
		col, ok := p.parseExpression(LOWEST)
		if !ok {
			return nil, false
		}
		columns = append(columns, col)

		if !p.peekTokenIs(COMMA) {
			break
		}
		p.nextToken() // consume comma
		p.nextToken() // move to next column
	}

	return &GroupByClause{Columns: columns}, true
}

func (p *Parser) parseHavingClause() (*HavingClause, bool) {
	p.nextToken()
	condition, ok := p.parseExpression(LOWEST)
	if !ok {
		return nil, false
	}

	return &HavingClause{Condition: condition}, true
}

func (p *Parser) parseOrderByClause() (*OrderByClause, bool) {
	if !p.expectPeek(BY) {
		return nil, false
	}

	var items []OrderByItem
	p.nextToken()

	for {
		expression, ok := p.parseExpression(LOWEST)
		if !ok {
			return nil, false
		}

		direction := AscendingOrder
		if p.peekTokenIs(IDENT) {
			p.nextToken()
			switch strings.ToUpper(p.curToken.Literal) {
			case AscOrder:
				direction = AscendingOrder
			case DescOrder:
				direction = DescendingOrder
			default:
				p.addError(fmt.Sprintf("invalid sort direction: %s", p.curToken.Literal))
				return nil, false
			}
		}

		items = append(items, OrderByItem{
			Expression: expression,
			Direction:  direction,
		})

		if !p.peekTokenIs(COMMA) {
			break
		}
		p.nextToken() // consume comma
		p.nextToken() // move to next item
	}

	return &OrderByClause{OrderItems: items}, true
}

func (p *Parser) parseLimitClause() (*LimitClause, bool) {
	if !p.expectPeek(INT) {
		return nil, false
	}

	count, err := strconv.ParseInt(p.curToken.Literal, 10, 64)
	if err != nil {
		p.addError(fmt.Sprintf("could not parse LIMIT value: %s", p.curToken.Literal))
		return nil, false
	}

	// Validate LIMIT value range to prevent overflow issues
	if count < 0 {
		p.addError(fmt.Sprintf("LIMIT value cannot be negative: %d", count))
		return nil, false
	}

	limitClause := &LimitClause{Count: count}

	// Check for OFFSET
	if p.peekTokenIs(OFFSET) {
		p.nextToken()
		if !p.expectPeek(INT) {
			return nil, false
		}

		offset, err := strconv.ParseInt(p.curToken.Literal, 10, 64)
		if err != nil {
			p.addError(fmt.Sprintf("could not parse OFFSET value: %s", p.curToken.Literal))
			return nil, false
		}

		// Validate OFFSET value range to prevent overflow issues
		if offset < 0 {
			p.addError(fmt.Sprintf("OFFSET value cannot be negative: %d", offset))
			return nil, false
		}

		limitClause.Offset = offset
	}

	return limitClause, true
}

// parseStandaloneOffsetClause parses an OFFSET clause without a LIMIT.
func (p *Parser) parseStandaloneOffsetClause() (*LimitClause, bool) {
	if !p.expectPeek(INT) {
		return nil, false
	}

	offset, err := strconv.ParseInt(p.curToken.Literal, 10, 64)
	if err != nil {
		p.addError(fmt.Sprintf("could not parse OFFSET value: %s", p.curToken.Literal))
		return nil, false
	}

	// Validate OFFSET value range to prevent overflow issues
	if offset < 0 {
		p.addError(fmt.Sprintf("OFFSET value cannot be negative: %d", offset))
		return nil, false
	}

	// For standalone OFFSET, we create a LimitClause with Count=OffsetOnlyLimit to indicate no limit
	// This allows the executor to distinguish between LIMIT+OFFSET and OFFSET-only
	limitClause := &LimitClause{
		Count:  OffsetOnlyLimit, // Special value indicating no LIMIT constraint
		Offset: offset,
	}

	return limitClause, true
}

// ParseSQL is the main entry point for SQL parsing.
func ParseSQL(input string) (SQLStatement, error) {
	lexer := NewLexer(input)
	parser := NewParser(lexer)

	stmt := parser.ParseSQL()
	if len(parser.Errors()) > 0 {
		return nil, fmt.Errorf("parse errors: %s", strings.Join(parser.Errors(), "; "))
	}

	return stmt, nil
}
