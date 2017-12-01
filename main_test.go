package main

import (
	"fmt"
	"testing"

	"github.com/fatih/color"
)

func TestDestParsing(t *testing.T) {
	type TestCase struct {
		name      string
		input     string
		expBucket string
		expPath   string
	}

	tests := []TestCase{
		{"Full bucket with file", "databag:/storage/notes.txt", "databag", "storage/notes.txt"},
		{"Bucket with a root", "backups:/", "backups", ""},
		{"End slash should be preserved", "backups:/test/dir/", "backups", "test/dir/"},
		{"No path specified", "backups:", "backups", ""},
		{"No path specified and no colon", "backups", "backups", ""},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			bucket, path := parseDest(tc.input)
			if bucket != tc.expBucket {
				t.Errorf("Expected to parse '%s' as destination bucket, but got: '%s'", tc.expBucket, bucket)
			}
			if path != tc.expPath {
				t.Errorf("Expected to parse '%s' as path, but got: '%s'", tc.expPath, path)
			}
		})
	}
}

func TestLabels(t *testing.T) {
	color.NoColor = true
	type testCase struct {
		name string
		f    func() string
	}

	tests := []testCase{
		{"Google", googlePrint},
		{"Amazon", amazonPrint},
	}

	for _, tc := range tests {
		t.Run(fmt.Sprintf("Label for %s", tc.name), func(t *testing.T) {
			if tc.f() != tc.name {
				t.Errorf("expected '%s', got '%s'", tc.name, tc.f())
			}
		})
	}
}
