package cloud

import (
	"context"
	"fmt"
	"strings"

	ini "gopkg.in/ini.v1"
)

// Backuper defines basic cloud backup operations
type Backuper interface {
	Upload(ctx context.Context, file string, dest string) error
}

// GetKeyValues gets values for the list of keys expected in INI file
func GetKeyValues(keys []string, cnf *ini.Section) (values map[string]string, err error) {
	values = make(map[string]string)
	var missing []string
	var empty []string
	for _, key := range keys {
		if !cnf.Haskey(key) {
			missing = append(missing, key)
			continue
		}
		v := strings.TrimSpace(cnf.Key(key).Value())
		if v == "" {
			empty = append(empty, key)
			continue
		}
		values[key] = v
	}
	if len(missing) > 0 {
		err = fmt.Errorf("missing required config entries: %s", strings.Join(missing, ","))
	}
	if len(empty) > 0 {
		err = fmt.Errorf("required config entries with empty values: %s%v", strings.Join(empty, ","), ","+err.Error())
	}
	return values, err
}
