package main

import (
	"fmt"
	"testing"
)

// TestHelloName calls greetings.Hello with a name, checking
// for a valid return value.
func TestGenerateCountQueries(t *testing.T) {
	str, _ := generateCountQueries("metadata.json")
	fmt.Printf("out %s\n", str)
	// name := "Gladys"
	// want := regexp.MustCompile(`\b` + name + `\b`)
	// msg, err := Hello("Gladys")
	// if !want.MatchString(msg) || err != nil {
	// 	t.Fatalf(`Hello("Gladys") = %q, %v, want match for %#q, nil`, msg, err, want)
	// }
}
