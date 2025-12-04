package utils

import "strings"

func StringIsEmptyOrWhitespace(s string) bool {
	return len(strings.TrimSpace(s)) == 0
}
