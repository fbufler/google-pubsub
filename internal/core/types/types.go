package types

import (
	"regexp"
	"strings"
)

const fqdnRegexValue = `^[a-zA-Z_][a-zA-Z0-9-_.~+%/]{2,254}$`

var fqdnRegex = regexp.MustCompile(fqdnRegexValue)

type FQDN string

func (n FQDN) String() string {
	return string(n)
}

func (n FQDN) IsValid() bool {
	s := n.String()
	// For full resource paths (projects/P/topics/T) check only the resource ID segment.
	// For bare names, reject the reserved "goog" prefix.
	if !strings.Contains(s, "/") && strings.HasPrefix(s, "goog") {
		return false
	}
	return fqdnRegex.MatchString(s)
}

const labelKeyRegexValue = `^[a-z][a-z0-9-]{0,62}$`

var labelKeyRegex = regexp.MustCompile(labelKeyRegexValue)

type Labels map[string]string

func (l Labels) IsValid() bool {
	if len(l) > 64 {
		return false
	}
	for k, v := range l {
		if !labelKeyRegex.MatchString(k) || len(v) > 63 {
			return false
		}
	}
	return true
}

const cmekRegexValue = `^projects/[a-zA-Z0-9-_.~+%]{2,254}/locations/[a-zA-Z0-9-_.~+%]{2,254}/keyRings/[a-zA-Z0-9-_.~+%]{2,254}/cryptoKeys/[a-zA-Z0-9-_.~+%]{2,254}$`

var cmekRegex = regexp.MustCompile(cmekRegexValue)

type CMEK string

func (c CMEK) String() string {
	return string(c)
}

func (c CMEK) IsValid() bool {
	return cmekRegex.MatchString(c.String())
}
