package usecases

import (
	"strings"
	"unicode"
)

// matchesFilter evaluates a PubSub filter expression against message attributes.
// Returns true when the filter is empty or the message matches the expression.
//
// Supported syntax (subset of the PubSub filter language):
//
//	attributes:key            — key exists
//	attributes.key = "value"  — equality
//	attributes.key != "value" — inequality
//	hasPrefix(attributes.key, "value") — prefix match
//	NOT expr  /  -expr
//	expr AND expr
//	expr OR expr
//	( expr )
func matchesFilter(filter string, attrs map[string]string) bool {
	if filter == "" {
		return true
	}
	p := &filterParser{input: filter, attrs: attrs}
	result := p.parseOr()
	return result
}

type filterParser struct {
	input string
	pos   int
	attrs map[string]string
}

func (p *filterParser) skipSpace() {
	for p.pos < len(p.input) && unicode.IsSpace(rune(p.input[p.pos])) {
		p.pos++
	}
}

func (p *filterParser) peek() byte {
	p.skipSpace()
	if p.pos >= len(p.input) {
		return 0
	}
	return p.input[p.pos]
}

func (p *filterParser) readIdent() string {
	p.skipSpace()
	start := p.pos
	for p.pos < len(p.input) {
		c := p.input[p.pos]
		if !unicode.IsLetter(rune(c)) && !unicode.IsDigit(rune(c)) && c != '_' && c != '-' {
			break
		}
		p.pos++
	}
	return p.input[start:p.pos]
}

// readString reads a double-quoted string literal and returns its content.
func (p *filterParser) readString() string {
	p.skipSpace()
	if p.pos >= len(p.input) || p.input[p.pos] != '"' {
		return ""
	}
	p.pos++ // consume opening quote
	var sb strings.Builder
	for p.pos < len(p.input) {
		c := p.input[p.pos]
		p.pos++
		if c == '"' {
			break
		}
		if c == '\\' && p.pos < len(p.input) {
			sb.WriteByte(p.input[p.pos])
			p.pos++
			continue
		}
		sb.WriteByte(c)
	}
	return sb.String()
}

// consume advances past a specific expected byte.
func (p *filterParser) consume(b byte) bool {
	p.skipSpace()
	if p.pos < len(p.input) && p.input[p.pos] == b {
		p.pos++
		return true
	}
	return false
}

// parseOr handles expr OR expr (lowest precedence).
func (p *filterParser) parseOr() bool {
	left := p.parseAnd()
	for {
		saved := p.pos
		p.skipSpace()
		if strings.HasPrefix(p.input[p.pos:], "OR") && !isIdentChar(p.input, p.pos+2) {
			p.pos += 2
			right := p.parseAnd()
			left = left || right
		} else {
			p.pos = saved
			break
		}
	}
	return left
}

// parseAnd handles expr AND expr.
func (p *filterParser) parseAnd() bool {
	left := p.parseNot()
	for {
		saved := p.pos
		p.skipSpace()
		if strings.HasPrefix(p.input[p.pos:], "AND") && !isIdentChar(p.input, p.pos+3) {
			p.pos += 3
			right := p.parseNot()
			left = left && right
		} else {
			p.pos = saved
			break
		}
	}
	return left
}

// parseNot handles NOT expr and -expr.
func (p *filterParser) parseNot() bool {
	p.skipSpace()
	if strings.HasPrefix(p.input[p.pos:], "NOT") && !isIdentChar(p.input, p.pos+3) {
		p.pos += 3
		return !p.parseNot()
	}
	if p.pos < len(p.input) && p.input[p.pos] == '-' {
		p.pos++
		return !p.parseNot()
	}
	return p.parsePrimary()
}

// parsePrimary handles atoms: parenthesised expressions, function calls, and field comparisons.
func (p *filterParser) parsePrimary() bool {
	p.skipSpace()
	if p.peek() == '(' {
		p.pos++
		result := p.parseOr()
		p.consume(')')
		return result
	}

	// hasPrefix(attributes.key, "value")
	if strings.HasPrefix(p.input[p.pos:], "hasPrefix(") {
		p.pos += len("hasPrefix(")
		_ = p.readIdent() // "attributes"
		p.consume('.')
		key := p.readIdent()
		p.consume(',')
		val := p.readString()
		p.consume(')')
		v, ok := p.attrs[key]
		return ok && strings.HasPrefix(v, val)
	}

	// attributes:key  or  attributes.key OP "value"
	base := p.readIdent()
	if base != "attributes" {
		return false
	}

	p.skipSpace()
	if p.pos >= len(p.input) {
		return false
	}

	switch p.input[p.pos] {
	case ':':
		p.pos++
		key := p.readIdent()
		_, ok := p.attrs[key]
		return ok
	case '.':
		p.pos++
		key := p.readIdent()
		p.skipSpace()
		op := p.readOp()
		val := p.readString()
		v, ok := p.attrs[key]
		switch op {
		case "=":
			return ok && v == val
		case "!=":
			return !ok || v != val
		case "<":
			return ok && v < val
		case "<=":
			return ok && v <= val
		case ">":
			return ok && v > val
		case ">=":
			return ok && v >= val
		}
	}
	return false
}

func (p *filterParser) readOp() string {
	p.skipSpace()
	if p.pos >= len(p.input) {
		return ""
	}
	switch p.input[p.pos] {
	case '=':
		p.pos++
		return "="
	case '!':
		if p.pos+1 < len(p.input) && p.input[p.pos+1] == '=' {
			p.pos += 2
			return "!="
		}
	case '<':
		if p.pos+1 < len(p.input) && p.input[p.pos+1] == '=' {
			p.pos += 2
			return "<="
		}
		p.pos++
		return "<"
	case '>':
		if p.pos+1 < len(p.input) && p.input[p.pos+1] == '=' {
			p.pos += 2
			return ">="
		}
		p.pos++
		return ">"
	}
	return ""
}

// isIdentChar reports whether the character at position i in s is an identifier character.
func isIdentChar(s string, i int) bool {
	if i >= len(s) {
		return false
	}
	c := rune(s[i])
	return unicode.IsLetter(c) || unicode.IsDigit(c) || c == '_'
}
