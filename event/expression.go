package event

import (
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"unicode"
)

// EvalContext is the data namespace exposed to event expressions.
type EvalContext struct {
	Data       map[string]interface{}
	LastValue  map[string]interface{}
	Property   map[string]interface{}
	Connection map[string]interface{}
}

// Expression is a parsed boolean expression.
type Expression struct {
	root exprNode
}

// Evaluate evaluates an expression. known is false when a referenced value is
// missing or cannot be converted to a boolean. The engine treats unknown
// conditions as non-matching and can then select an explicit fallback rule.
func (e *Expression) Evaluate(ctx EvalContext) (value bool, known bool) {
	if e == nil || e.root == nil {
		return false, false
	}
	result := e.root.eval(ctx)
	return asBool(result)
}

// References returns all variable references used by an expression.
func (e *Expression) References() []string {
	if e == nil || e.root == nil {
		return nil
	}
	set := make(map[string]struct{})
	e.root.references(set)
	refs := make([]string, 0, len(set))
	for ref := range set {
		refs = append(refs, ref)
	}
	sort.Strings(refs)
	return refs
}

func ParseExpression(raw string) (*Expression, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}
	parser := &exprParser{lexer: newExprLexer(raw)}
	root, err := parser.parseOr()
	if err != nil {
		return nil, err
	}
	if token := parser.peek(); token.kind != tokenEOF {
		return nil, fmt.Errorf("unexpected token %q", token.text)
	}
	return &Expression{root: root}, nil
}

type evalValue struct {
	value interface{}
	known bool
}

type exprNode interface {
	eval(EvalContext) evalValue
	references(map[string]struct{})
}

type literalNode struct{ value interface{} }

func (n literalNode) eval(EvalContext) evalValue     { return evalValue{value: n.value, known: true} }
func (n literalNode) references(map[string]struct{}) {}

type identifierNode struct{ name string }

func (n identifierNode) eval(ctx EvalContext) evalValue {
	parts := strings.Split(n.name, ".")
	if len(parts) == 0 {
		return evalValue{}
	}
	var current interface{}
	switch parts[0] {
	case "data":
		current = ctx.Data
	case "LAST_VALUE":
		current = ctx.LastValue
	case "property":
		current = ctx.Property
	case "connection":
		current = ctx.Connection
	default:
		return evalValue{}
	}
	for _, part := range parts[1:] {
		value, ok := lookupMap(current, part)
		if !ok {
			return evalValue{}
		}
		current = value
	}
	return evalValue{value: current, known: true}
}

func (n identifierNode) references(set map[string]struct{}) {
	set[n.name] = struct{}{}
}

type unaryNode struct {
	op    tokenKind
	child exprNode
}

func (n unaryNode) eval(ctx EvalContext) evalValue {
	value := n.child.eval(ctx)
	if n.op != tokenNot {
		return evalValue{}
	}
	boolValue, known := asBool(value)
	if !known {
		return evalValue{}
	}
	return evalValue{value: !boolValue, known: true}
}

func (n unaryNode) references(set map[string]struct{}) { n.child.references(set) }

type logicalNode struct {
	op          tokenKind
	left, right exprNode
}

func (n logicalNode) eval(ctx EvalContext) evalValue {
	left := n.left.eval(ctx)
	leftBool, leftKnown := asBool(left)
	if n.op == tokenAnd && leftKnown && !leftBool {
		return evalValue{value: false, known: true}
	}
	if n.op == tokenOr && leftKnown && leftBool {
		return evalValue{value: true, known: true}
	}
	right := n.right.eval(ctx)
	rightBool, rightKnown := asBool(right)
	if n.op == tokenAnd {
		if rightKnown && !rightBool {
			return evalValue{value: false, known: true}
		}
		if leftKnown && rightKnown {
			return evalValue{value: leftBool && rightBool, known: true}
		}
		return evalValue{}
	}
	if rightKnown && rightBool {
		return evalValue{value: true, known: true}
	}
	if leftKnown && rightKnown {
		return evalValue{value: leftBool || rightBool, known: true}
	}
	return evalValue{}
}

func (n logicalNode) references(set map[string]struct{}) {
	n.left.references(set)
	n.right.references(set)
}

type comparisonNode struct {
	op          tokenKind
	left, right exprNode
}

func (n comparisonNode) eval(ctx EvalContext) evalValue {
	left := n.left.eval(ctx)
	right := n.right.eval(ctx)
	if !left.known || !right.known {
		return evalValue{}
	}
	equal, comparable := equalValues(left.value, right.value)
	if !comparable {
		return evalValue{}
	}
	if n.op == tokenEqual {
		return evalValue{value: equal, known: true}
	}
	if n.op == tokenNotEqual {
		return evalValue{value: !equal, known: true}
	}
	leftNumber, leftOK := numberValue(left.value)
	rightNumber, rightOK := numberValue(right.value)
	if !leftOK || !rightOK {
		return evalValue{}
	}
	var result bool
	switch n.op {
	case tokenGreater:
		result = leftNumber > rightNumber
	case tokenGreaterEqual:
		result = leftNumber >= rightNumber
	case tokenLess:
		result = leftNumber < rightNumber
	case tokenLessEqual:
		result = leftNumber <= rightNumber
	default:
		return evalValue{}
	}
	return evalValue{value: result, known: true}
}

func (n comparisonNode) references(set map[string]struct{}) {
	n.left.references(set)
	n.right.references(set)
}

func asBool(value evalValue) (bool, bool) {
	if !value.known {
		return false, false
	}
	switch v := value.value.(type) {
	case bool:
		return v, true
	case string:
		parsed, err := strconv.ParseBool(strings.TrimSpace(v))
		if err == nil {
			return parsed, true
		}
	case int:
		return v != 0, true
	case int8:
		return v != 0, true
	case int16:
		return v != 0, true
	case int32:
		return v != 0, true
	case int64:
		return v != 0, true
	case uint:
		return v != 0, true
	case uint8:
		return v != 0, true
	case uint16:
		return v != 0, true
	case uint32:
		return v != 0, true
	case uint64:
		return v != 0, true
	case float32:
		return v != 0, true
	case float64:
		return v != 0, true
	}
	return false, false
}

func equalValues(left, right interface{}) (bool, bool) {
	if left == nil || right == nil {
		return left == nil && right == nil, true
	}
	if equal, comparable := collectionEqualsScalar(left, right); comparable {
		return equal, true
	}
	if equal, comparable := collectionEqualsScalar(right, left); comparable {
		return equal, true
	}
	if leftNumber, ok := numberValue(left); ok {
		if rightNumber, rightOK := numberValue(right); rightOK {
			return leftNumber == rightNumber, true
		}
	}
	if leftBool, ok := boolValue(left); ok {
		if rightBool, rightOK := boolValue(right); rightOK {
			return leftBool == rightBool, true
		}
	}
	if leftString, ok := left.(string); ok {
		if rightString, rightOK := right.(string); rightOK {
			return leftString == rightString, true
		}
	}
	if reflect.TypeOf(left) == reflect.TypeOf(right) {
		return reflect.DeepEqual(left, right), true
	}
	return false, false
}

func collectionEqualsScalar(collection, scalar interface{}) (bool, bool) {
	value := reflect.ValueOf(collection)
	if value.Kind() != reflect.Slice && value.Kind() != reflect.Array {
		return false, false
	}
	for index := 0; index < value.Len(); index++ {
		item := value.Index(index).Interface()
		equal, comparable := equalValues(item, scalar)
		if !comparable {
			return false, false
		}
		if !equal {
			return false, true
		}
	}
	return true, true
}

func boolValue(value interface{}) (bool, bool) {
	switch v := value.(type) {
	case bool:
		return v, true
	case string:
		parsed, err := strconv.ParseBool(strings.TrimSpace(v))
		return parsed, err == nil
	default:
		return false, false
	}
}

func numberValue(value interface{}) (float64, bool) {
	switch v := value.(type) {
	case int:
		return float64(v), true
	case int8:
		return float64(v), true
	case int16:
		return float64(v), true
	case int32:
		return float64(v), true
	case int64:
		return float64(v), true
	case uint:
		return float64(v), true
	case uint8:
		return float64(v), true
	case uint16:
		return float64(v), true
	case uint32:
		return float64(v), true
	case uint64:
		return float64(v), true
	case float32:
		return float64(v), true
	case float64:
		return v, true
	case string:
		parsed, err := strconv.ParseFloat(strings.TrimSpace(v), 64)
		return parsed, err == nil
	default:
		return 0, false
	}
}

func lookupMap(current interface{}, key string) (interface{}, bool) {
	switch value := current.(type) {
	case map[string]interface{}:
		result, ok := value[key]
		return result, ok
	case map[interface{}]interface{}:
		result, ok := value[key]
		return result, ok
	default:
		return nil, false
	}
}

type tokenKind int

const (
	tokenEOF tokenKind = iota
	tokenIdentifier
	tokenNumber
	tokenString
	tokenTrue
	tokenFalse
	tokenNull
	tokenEqual
	tokenNotEqual
	tokenGreater
	tokenGreaterEqual
	tokenLess
	tokenLessEqual
	tokenAnd
	tokenOr
	tokenNot
	tokenLeftParen
	tokenRightParen
)

type exprToken struct {
	kind  tokenKind
	text  string
	value interface{}
}

type exprLexer struct {
	runes []rune
	pos   int
}

func newExprLexer(raw string) *exprLexer { return &exprLexer{runes: []rune(raw)} }

func (l *exprLexer) next() (exprToken, error) {
	for l.pos < len(l.runes) && unicode.IsSpace(l.runes[l.pos]) {
		l.pos++
	}
	if l.pos >= len(l.runes) {
		return exprToken{kind: tokenEOF}, nil
	}
	start := l.pos
	ch := l.runes[l.pos]
	if unicode.IsLetter(ch) || ch == '_' {
		l.pos++
		for l.pos < len(l.runes) {
			current := l.runes[l.pos]
			if unicode.IsLetter(current) || unicode.IsDigit(current) || current == '_' || current == '.' {
				l.pos++
				continue
			}
			break
		}
		text := string(l.runes[start:l.pos])
		switch strings.ToLower(text) {
		case "true":
			return exprToken{kind: tokenTrue, text: text, value: true}, nil
		case "false":
			return exprToken{kind: tokenFalse, text: text, value: false}, nil
		case "null", "nil":
			return exprToken{kind: tokenNull, text: text, value: nil}, nil
		default:
			return exprToken{kind: tokenIdentifier, text: text}, nil
		}
	}
	if unicode.IsDigit(ch) || ch == '-' || ch == '+' {
		l.pos++
		for l.pos < len(l.runes) {
			current := l.runes[l.pos]
			if unicode.IsDigit(current) || current == '.' || current == 'e' || current == 'E' || current == '+' || current == '-' {
				l.pos++
				continue
			}
			break
		}
		text := string(l.runes[start:l.pos])
		value, err := strconv.ParseFloat(text, 64)
		if err != nil {
			return exprToken{}, fmt.Errorf("invalid number %q", text)
		}
		return exprToken{kind: tokenNumber, text: text, value: value}, nil
	}
	if ch == '\'' || ch == '"' {
		quote := ch
		l.pos++
		var builder strings.Builder
		for l.pos < len(l.runes) {
			current := l.runes[l.pos]
			l.pos++
			if current == quote {
				return exprToken{kind: tokenString, text: string(l.runes[start:l.pos]), value: builder.String()}, nil
			}
			if current == '\\' && l.pos < len(l.runes) {
				next := l.runes[l.pos]
				l.pos++
				builder.WriteRune(next)
				continue
			}
			builder.WriteRune(current)
		}
		return exprToken{}, fmt.Errorf("unterminated string")
	}
	for _, operator := range []struct {
		text string
		kind tokenKind
	}{
		{"==", tokenEqual}, {"!=", tokenNotEqual}, {">=", tokenGreaterEqual}, {"<=", tokenLessEqual}, {"&&", tokenAnd}, {"||", tokenOr},
	} {
		if strings.HasPrefix(string(l.runes[l.pos:]), operator.text) {
			l.pos += len([]rune(operator.text))
			return exprToken{kind: operator.kind, text: operator.text}, nil
		}
	}
	l.pos++
	switch ch {
	case '>':
		return exprToken{kind: tokenGreater, text: ">"}, nil
	case '<':
		return exprToken{kind: tokenLess, text: "<"}, nil
	case '!':
		return exprToken{kind: tokenNot, text: "!"}, nil
	case '(':
		return exprToken{kind: tokenLeftParen, text: "("}, nil
	case ')':
		return exprToken{kind: tokenRightParen, text: ")"}, nil
	default:
		return exprToken{}, fmt.Errorf("unexpected character %q", ch)
	}
}

type exprParser struct {
	lexer  *exprLexer
	buffer *exprToken
}

func (p *exprParser) peek() exprToken {
	if p.buffer != nil {
		return *p.buffer
	}
	token, err := p.lexer.next()
	if err != nil {
		p.buffer = &exprToken{kind: tokenEOF, text: err.Error()}
		return *p.buffer
	}
	p.buffer = &token
	return token
}

func (p *exprParser) take() (exprToken, error) {
	token := p.peek()
	p.buffer = nil
	if token.kind == tokenEOF && token.text != "" {
		return token, fmt.Errorf("%s", token.text)
	}
	return token, nil
}

func (p *exprParser) parseOr() (exprNode, error) {
	left, err := p.parseAnd()
	if err != nil {
		return nil, err
	}
	for p.peek().kind == tokenOr {
		_, _ = p.take()
		right, err := p.parseAnd()
		if err != nil {
			return nil, err
		}
		left = logicalNode{op: tokenOr, left: left, right: right}
	}
	return left, nil
}

func (p *exprParser) parseAnd() (exprNode, error) {
	left, err := p.parseUnary()
	if err != nil {
		return nil, err
	}
	for p.peek().kind == tokenAnd {
		_, _ = p.take()
		right, err := p.parseUnary()
		if err != nil {
			return nil, err
		}
		left = logicalNode{op: tokenAnd, left: left, right: right}
	}
	return left, nil
}

func (p *exprParser) parseUnary() (exprNode, error) {
	if p.peek().kind == tokenNot {
		_, _ = p.take()
		child, err := p.parseUnary()
		if err != nil {
			return nil, err
		}
		return unaryNode{op: tokenNot, child: child}, nil
	}
	return p.parseComparison()
}

func (p *exprParser) parseComparison() (exprNode, error) {
	left, err := p.parsePrimary()
	if err != nil {
		return nil, err
	}
	kind := p.peek().kind
	if kind != tokenEqual && kind != tokenNotEqual && kind != tokenGreater && kind != tokenGreaterEqual && kind != tokenLess && kind != tokenLessEqual {
		return left, nil
	}
	_, _ = p.take()
	right, err := p.parsePrimary()
	if err != nil {
		return nil, err
	}
	return comparisonNode{op: kind, left: left, right: right}, nil
}

func (p *exprParser) parsePrimary() (exprNode, error) {
	token, err := p.take()
	if err != nil {
		return nil, err
	}
	switch token.kind {
	case tokenIdentifier:
		return identifierNode{name: token.text}, nil
	case tokenNumber, tokenString, tokenTrue, tokenFalse, tokenNull:
		return literalNode{value: token.value}, nil
	case tokenLeftParen:
		value, err := p.parseOr()
		if err != nil {
			return nil, err
		}
		if closing := p.peek(); closing.kind != tokenRightParen {
			return nil, fmt.Errorf("expected ')' but got %q", closing.text)
		}
		_, _ = p.take()
		return value, nil
	default:
		return nil, fmt.Errorf("expected value but got %q", token.text)
	}
}
