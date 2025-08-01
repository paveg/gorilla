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
	// EOF represents end of file token.
	EOF TokenType = iota
	ILLEGAL

	// IDENT represents identifier tokens like column names, table names, function names.
	IDENT
	INT    // integers
	FLOAT  // floating point numbers
	STRING // string literals

	// SELECT represents the SELECT keyword.
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

	// EQ represents the equals operator (=).
	EQ
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

	// COMMA represents the comma delimiter (,).
	COMMA
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
	case '=', '!', '<', '>':
		tok = l.tokenizeComparisonOperator()
	case '+', '-', '*', '/', '%':
		tok = l.tokenizeArithmeticOperator()
	case ',', ';', '(', ')', '.':
		tok = l.tokenizeDelimiter()
	case '\'', '"':
		tok = l.tokenizeString()
	case 0:
		tok = l.tokenizeEOF()
	default:
		if isLetter(l.ch) {
			tok = l.tokenizeIdentifier()
			return tok // early return to avoid readChar() call
		} else if isDigit(l.ch) {
			tok = l.tokenizeNumber()
			return tok // early return to avoid readChar() call
		}
		tok = Token{Type: ILLEGAL, Literal: string(l.ch), Position: l.position}
	}

	l.readChar()
	return tok
}

// tokenizeComparisonOperator handles comparison operators (=, !=, <, <=, >, >=, <>).
func (l *Lexer) tokenizeComparisonOperator() Token {
	switch l.ch {
	case '=':
		return Token{Type: EQ, Literal: string(l.ch), Position: l.position}
	case '!':
		if l.peekChar() == '=' {
			ch := l.ch
			l.readChar()
			return Token{Type: NE, Literal: string(ch) + string(l.ch), Position: l.position - 1}
		}
		return Token{Type: ILLEGAL, Literal: string(l.ch), Position: l.position}
	case '<':
		switch l.peekChar() {
		case '=':
			ch := l.ch
			l.readChar()
			return Token{Type: LE, Literal: string(ch) + string(l.ch), Position: l.position - 1}
		case '>':
			ch := l.ch
			l.readChar()
			return Token{Type: NE, Literal: string(ch) + string(l.ch), Position: l.position - 1}
		default:
			return Token{Type: LT, Literal: string(l.ch), Position: l.position}
		}
	case '>':
		if l.peekChar() == '=' {
			ch := l.ch
			l.readChar()
			return Token{Type: GE, Literal: string(ch) + string(l.ch), Position: l.position - 1}
		}
		return Token{Type: GT, Literal: string(l.ch), Position: l.position}
	default:
		return Token{Type: ILLEGAL, Literal: string(l.ch), Position: l.position}
	}
}

// tokenizeArithmeticOperator handles arithmetic operators (+, -, *, /, %).
func (l *Lexer) tokenizeArithmeticOperator() Token {
	switch l.ch {
	case '+':
		return Token{Type: PLUS, Literal: string(l.ch), Position: l.position}
	case '-':
		return Token{Type: MINUS, Literal: string(l.ch), Position: l.position}
	case '*':
		return Token{Type: MULT, Literal: string(l.ch), Position: l.position}
	case '/':
		return Token{Type: DIV, Literal: string(l.ch), Position: l.position}
	case '%':
		return Token{Type: MOD, Literal: string(l.ch), Position: l.position}
	default:
		return Token{Type: ILLEGAL, Literal: string(l.ch), Position: l.position}
	}
}

// tokenizeDelimiter handles delimiter tokens (,, ;, (, ), .).
func (l *Lexer) tokenizeDelimiter() Token {
	switch l.ch {
	case ',':
		return Token{Type: COMMA, Literal: string(l.ch), Position: l.position}
	case ';':
		return Token{Type: SEMICOLON, Literal: string(l.ch), Position: l.position}
	case '(':
		return Token{Type: LPAREN, Literal: string(l.ch), Position: l.position}
	case ')':
		return Token{Type: RPAREN, Literal: string(l.ch), Position: l.position}
	case '.':
		return Token{Type: DOT, Literal: string(l.ch), Position: l.position}
	default:
		return Token{Type: ILLEGAL, Literal: string(l.ch), Position: l.position}
	}
}

// tokenizeString handles string literals (' and " quoted strings).
func (l *Lexer) tokenizeString() Token {
	return Token{
		Type:     STRING,
		Literal:  l.readString(),
		Position: l.position,
	}
}

// tokenizeEOF handles end-of-file token.
func (l *Lexer) tokenizeEOF() Token {
	return Token{
		Literal:  "",
		Type:     EOF,
		Position: l.position,
	}
}

// tokenizeIdentifier handles identifier tokens (keywords, column names, etc.).
func (l *Lexer) tokenizeIdentifier() Token {
	position := l.position
	literal := l.readIdentifier()
	tokenType := lookupIdent(literal)
	return Token{
		Type:     tokenType,
		Literal:  literal,
		Position: position,
	}
}

// tokenizeNumber handles numeric tokens (integers and floats).
func (l *Lexer) tokenizeNumber() Token {
	position := l.position
	tokenType, literal := l.readNumber()
	return Token{
		Type:     tokenType,
		Literal:  literal,
		Position: position,
	}
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

// getKeywordsMap returns the keywords map.
func getKeywordsMap() map[string]TokenType {
	return map[string]TokenType{
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
}

// getPrecedencesMap returns the precedences map.
func getPrecedencesMap() map[TokenType]int {
	//nolint:exhaustive // Only tokens with precedence need to be mapped
	return map[TokenType]int{
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
}

// lookupIdent checks if identifier is a keyword.
func lookupIdent(ident string) TokenType {
	if tok, ok := getKeywordsMap()[strings.ToUpper(ident)]; ok {
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
func (p *Parser) ParseSQL() Statement {
	//nolint:exhaustive // Parser only handles SELECT statements currently
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

	// Parse SELECT list.
	if !p.parseSelectListClause(stmt) {
		return nil
	}

	// Parse optional clauses in sequence.
	if !p.parseOptionalClauses(stmt) {
		return nil
	}

	return stmt
}

// parseSelectListClause parses the SELECT list and assigns it to the statement.
func (p *Parser) parseSelectListClause(stmt *SelectStatement) bool {
	selectList, ok := p.parseSelectList()
	if !ok {
		return false
	}
	stmt.SelectList = selectList
	return true
}

// parseOptionalClauses parses all optional clauses in order.
func (p *Parser) parseOptionalClauses(stmt *SelectStatement) bool {
	return p.parseFromClauseIfPresent(stmt) &&
		p.parseWhereClauseIfPresent(stmt) &&
		p.parseGroupByClauseIfPresent(stmt) &&
		p.parseHavingClauseIfPresent(stmt) &&
		p.parseOrderByClauseIfPresent(stmt) &&
		p.parseLimitOffsetClausesIfPresent(stmt)
}

// parseFromClauseIfPresent parses FROM clause if present.
func (p *Parser) parseFromClauseIfPresent(stmt *SelectStatement) bool {
	if !p.peekTokenIs(FROM) {
		return true
	}
	p.nextToken()
	fromClause, ok := p.parseFromClause()
	if !ok {
		return false
	}
	stmt.FromClause = fromClause
	return true
}

// parseWhereClauseIfPresent parses WHERE clause if present.
func (p *Parser) parseWhereClauseIfPresent(stmt *SelectStatement) bool {
	if !p.peekTokenIs(WHERE) {
		return true
	}
	p.nextToken()
	whereClause, ok := p.parseWhereClause()
	if !ok {
		return false
	}
	stmt.WhereClause = whereClause
	return true
}

// parseGroupByClauseIfPresent parses GROUP BY clause if present.
func (p *Parser) parseGroupByClauseIfPresent(stmt *SelectStatement) bool {
	if !p.peekTokenIs(GROUP) {
		return true
	}
	p.nextToken()
	groupByClause, ok := p.parseGroupByClause()
	if !ok {
		return false
	}
	stmt.GroupByClause = groupByClause
	return true
}

// parseHavingClauseIfPresent parses HAVING clause if present.
func (p *Parser) parseHavingClauseIfPresent(stmt *SelectStatement) bool {
	if !p.peekTokenIs(HAVING) {
		return true
	}
	p.nextToken()
	havingClause, ok := p.parseHavingClause()
	if !ok {
		return false
	}
	stmt.HavingClause = havingClause
	return true
}

// parseOrderByClauseIfPresent parses ORDER BY clause if present.
func (p *Parser) parseOrderByClauseIfPresent(stmt *SelectStatement) bool {
	if !p.peekTokenIs(ORDER) {
		return true
	}
	p.nextToken()
	orderByClause, ok := p.parseOrderByClause()
	if !ok {
		return false
	}
	stmt.OrderByClause = orderByClause
	return true
}

// parseLimitOffsetClausesIfPresent parses LIMIT and OFFSET clauses if present.
func (p *Parser) parseLimitOffsetClausesIfPresent(stmt *SelectStatement) bool {
	if p.peekTokenIs(LIMIT) {
		return p.parseLimitClauseAndAssign(stmt)
	}
	if p.peekTokenIs(OFFSET) {
		return p.parseStandaloneOffsetClauseAndAssign(stmt)
	}
	return true
}

// parseLimitClauseAndAssign parses LIMIT clause and assigns it to the statement.
func (p *Parser) parseLimitClauseAndAssign(stmt *SelectStatement) bool {
	p.nextToken()
	limitClause, ok := p.parseLimitClause()
	if !ok {
		return false
	}
	stmt.LimitClause = limitClause
	return true
}

// parseStandaloneOffsetClauseAndAssign parses standalone OFFSET clause and assigns it to the statement.
func (p *Parser) parseStandaloneOffsetClauseAndAssign(stmt *SelectStatement) bool {
	p.nextToken()
	offsetClause, ok := p.parseStandaloneOffsetClause()
	if !ok {
		return false
	}
	stmt.LimitClause = offsetClause
	return true
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
	//nolint:exhaustive // Only specific tokens are valid aliases
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
	//nolint:exhaustive // Parser handles only specific prefix token types
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
	//nolint:exhaustive // Parser handles only specific infix token types
	switch p.peekToken.Type {
	case EQ, NE, LT, LE, GT, GE, PLUS, MINUS, MULT, DIV, MOD:
		p.nextToken()
		return p.parseBinaryExpression(left)
	case AND, OR:
		p.nextToken()
		return p.parseLogicalExpression(left)
	case LPAREN:
		if colExpr, ok := left.(*expr.ColumnExpr); ok {
			p.nextToken()
			return p.parseFunctionCallWithLeft(colExpr)
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
	if prec, ok := getPrecedencesMap()[p.peekToken.Type]; ok {
		return prec
	}
	return LOWEST
}

func (p *Parser) curPrecedence() int {
	if prec, ok := getPrecedencesMap()[p.curToken.Type]; ok {
		return prec
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

	return p.applyBinaryOperator(left, operator, right)
}

// applyBinaryOperator applies the binary operator to the left and right expressions.
func (p *Parser) applyBinaryOperator(left expr.Expr, operator string, right expr.Expr) (expr.Expr, bool) {
	switch operator {
	case "+":
		return p.applyArithmeticOperation(left, right, "addition",
			func(c *expr.ColumnExpr) expr.Expr { return c.Add(right) },
			func(b *expr.BinaryExpr) expr.Expr { return b.Add(right) },
			func(f *expr.FunctionExpr) expr.Expr { return f.Add(right) },
			func(a *expr.AggregationExpr) expr.Expr { return a.Add(right) })
	case "-":
		return p.applyArithmeticOperation(left, right, "subtraction",
			func(c *expr.ColumnExpr) expr.Expr { return c.Sub(right) },
			func(b *expr.BinaryExpr) expr.Expr { return b.Sub(right) },
			func(f *expr.FunctionExpr) expr.Expr { return f.Sub(right) },
			func(a *expr.AggregationExpr) expr.Expr { return a.Sub(right) })
	case "*":
		return p.applyArithmeticOperation(left, right, "multiplication",
			func(c *expr.ColumnExpr) expr.Expr { return c.Mul(right) },
			func(b *expr.BinaryExpr) expr.Expr { return b.Mul(right) },
			func(f *expr.FunctionExpr) expr.Expr { return f.Mul(right) },
			func(a *expr.AggregationExpr) expr.Expr { return a.Mul(right) })
	case "/":
		return p.applyArithmeticOperation(left, right, "division",
			func(c *expr.ColumnExpr) expr.Expr { return c.Div(right) },
			func(b *expr.BinaryExpr) expr.Expr { return b.Div(right) },
			func(f *expr.FunctionExpr) expr.Expr { return f.Div(right) },
			func(a *expr.AggregationExpr) expr.Expr { return a.Div(right) })
	case "%":
		p.addError("modulo operator (%) not supported yet")
		return nil, false
	case "=":
		return p.applyComparisonOperation(left, right, "equality",
			func(c *expr.ColumnExpr) expr.Expr { return c.Eq(right) },
			func(f *expr.FunctionExpr) expr.Expr { return f.Eq(right) },
			func(a *expr.AggregationExpr) expr.Expr { return a.Eq(right) })
	case "!=", "<>":
		return p.applyComparisonOperation(left, right, "not equal",
			func(c *expr.ColumnExpr) expr.Expr { return c.Ne(right) },
			func(f *expr.FunctionExpr) expr.Expr { return f.Ne(right) },
			func(a *expr.AggregationExpr) expr.Expr { return a.Ne(right) })
	case "<":
		return p.applyComparisonOperation(left, right, "less than",
			func(c *expr.ColumnExpr) expr.Expr { return c.Lt(right) },
			func(f *expr.FunctionExpr) expr.Expr { return f.Lt(right) },
			func(a *expr.AggregationExpr) expr.Expr { return a.Lt(right) })
	case "<=":
		return p.applyComparisonOperation(left, right, "less than or equal",
			func(c *expr.ColumnExpr) expr.Expr { return c.Le(right) },
			func(f *expr.FunctionExpr) expr.Expr { return f.Le(right) },
			func(a *expr.AggregationExpr) expr.Expr { return a.Le(right) })
	case ">":
		return p.applyComparisonOperation(left, right, "greater than",
			func(c *expr.ColumnExpr) expr.Expr { return c.Gt(right) },
			func(f *expr.FunctionExpr) expr.Expr { return f.Gt(right) },
			func(a *expr.AggregationExpr) expr.Expr { return a.Gt(right) })
	case ">=":
		return p.applyComparisonOperation(left, right, "greater than or equal",
			func(c *expr.ColumnExpr) expr.Expr { return c.Ge(right) },
			func(f *expr.FunctionExpr) expr.Expr { return f.Ge(right) },
			func(a *expr.AggregationExpr) expr.Expr { return a.Ge(right) })
	default:
		p.addError(fmt.Sprintf("unknown operator: %s", operator))
		return nil, false
	}
}

// applyArithmeticOperation applies arithmetic operations (supports BinaryExpr).
func (p *Parser) applyArithmeticOperation(left expr.Expr, _ expr.Expr, opName string,
	colFn func(*expr.ColumnExpr) expr.Expr,
	binFn func(*expr.BinaryExpr) expr.Expr,
	funFn func(*expr.FunctionExpr) expr.Expr,
	aggFn func(*expr.AggregationExpr) expr.Expr) (expr.Expr, bool) {
	switch l := left.(type) {
	case *expr.ColumnExpr:
		return colFn(l), true
	case *expr.BinaryExpr:
		return binFn(l), true
	case *expr.FunctionExpr:
		return funFn(l), true
	case *expr.AggregationExpr:
		return aggFn(l), true
	default:
		p.addError(fmt.Sprintf("unsupported expression type for %s: %T", opName, left))
		return nil, false
	}
}

// applyComparisonOperation applies comparison operations (no BinaryExpr support).
func (p *Parser) applyComparisonOperation(left expr.Expr, _ expr.Expr, opName string,
	colFn func(*expr.ColumnExpr) expr.Expr,
	funFn func(*expr.FunctionExpr) expr.Expr,
	aggFn func(*expr.AggregationExpr) expr.Expr) (expr.Expr, bool) {
	switch l := left.(type) {
	case *expr.ColumnExpr:
		return colFn(l), true
	case *expr.FunctionExpr:
		return funFn(l), true
	case *expr.AggregationExpr:
		return aggFn(l), true
	default:
		p.addError(fmt.Sprintf("unsupported expression type for %s: %T", opName, left))
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
		if bin, binOk := left.(*expr.BinaryExpr); binOk {
			return bin.And(right), true
		} else if fun, funOk := left.(*expr.FunctionExpr); funOk {
			return fun.And(right), true
		} else if agg, aggOk := left.(*expr.AggregationExpr); aggOk {
			return agg.And(right), true
		}
		p.addError(fmt.Sprintf("unsupported expression type for AND operation: %T", left))
		return nil, false
	case "OR":
		if bin, binOk := left.(*expr.BinaryExpr); binOk {
			return bin.Or(right), true
		} else if fun, funOk := left.(*expr.FunctionExpr); funOk {
			return fun.Or(right), true
		} else if agg, aggOk := left.(*expr.AggregationExpr); aggOk {
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

	// Convert SQL functions to Gorilla expressions using helper functions
	switch strings.ToUpper(functionName) {
	// Aggregation functions
	case CountFunction:
		return p.parseCountFunction(args)
	case SumFunction:
		return p.parseAggregationFunction("SUM", args, expr.Sum)
	case AvgFunction:
		return p.parseAggregationFunction("AVG", args, expr.Mean)
	case MinFunction:
		return p.parseAggregationFunction("MIN", args, expr.Min)
	case MaxFunction:
		return p.parseAggregationFunction("MAX", args, expr.Max)

	// String functions
	case UpperFunction:
		return p.parseColumnFunction("UPPER", args, (*expr.ColumnExpr).Upper)
	case LowerFunction:
		return p.parseColumnFunction("LOWER", args, (*expr.ColumnExpr).Lower)
	case LengthFunction:
		return p.parseColumnFunction("LENGTH", args, (*expr.ColumnExpr).Length)
	case TrimFunction:
		return p.parseColumnFunction("TRIM", args, (*expr.ColumnExpr).Trim)

	// Math functions
	case AbsFunction:
		return p.parseColumnFunction("ABS", args, (*expr.ColumnExpr).Abs)
	case RoundFunction:
		return p.parseColumnFunction("ROUND", args, (*expr.ColumnExpr).Round)
	case FloorFunction:
		return p.parseColumnFunction("FLOOR", args, (*expr.ColumnExpr).Floor)
	case CeilFunction:
		return p.parseColumnFunction("CEIL", args, (*expr.ColumnExpr).Ceil)
	default:
		// Create generic function expression for other functions
		return expr.NewFunction(functionName, args...), true
	}
}

// parseCountFunction handles the COUNT function with special logic for zero arguments.
func (p *Parser) parseCountFunction(args []expr.Expr) (expr.Expr, bool) {
	if len(args) == 0 {
		return expr.Count(expr.Lit(1)), true
	}
	return expr.Count(args[0]), true
}

// parseAggregationFunction handles aggregation functions that require exactly one argument.
func (p *Parser) parseAggregationFunction(
	name string,
	args []expr.Expr,
	exprFunc func(expr.Expr) *expr.AggregationExpr,
) (expr.Expr, bool) {
	if len(args) != 1 {
		p.addError(fmt.Sprintf("%s function requires exactly one argument", name))
		return nil, false
	}
	return exprFunc(args[0]), true
}

// parseColumnFunction handles functions that operate on column expressions.
func (p *Parser) parseColumnFunction(
	name string,
	args []expr.Expr,
	columnFunc func(*expr.ColumnExpr) *expr.FunctionExpr,
) (expr.Expr, bool) {
	if len(args) != 1 {
		p.addError(fmt.Sprintf("%s function requires exactly one argument", name))
		return nil, false
	}

	colExpr, ok := args[0].(*expr.ColumnExpr)
	if !ok {
		p.addError(fmt.Sprintf("%s function requires a column argument", name))
		return nil, false
	}

	return columnFunc(colExpr), true
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
		nextArg, nextOk := p.parseExpression(LOWEST)
		if !nextOk {
			return nil, false
		}
		args = append(args, nextArg)
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

		offset, offsetErr := strconv.ParseInt(p.curToken.Literal, 10, 64)
		if offsetErr != nil {
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
func ParseSQL(input string) (Statement, error) {
	lexer := NewLexer(input)
	parser := NewParser(lexer)

	stmt := parser.ParseSQL()
	if len(parser.Errors()) > 0 {
		return nil, fmt.Errorf("parse errors: %s", strings.Join(parser.Errors(), "; "))
	}

	return stmt, nil
}
