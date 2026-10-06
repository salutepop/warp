// Copyright 2026. Licensed under the GNU Affero General Public License v3.0 or later.
package bench

import (
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/minio/warp/pkg/generator"
)

// loadGetObjects reads a caller-owned, already prepared fixture. No S3 request
// occurs here, allowing page residency to be checked before the timed run.
func loadGetObjects(path string, expected int) (generator.Objects, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var manifest struct {
		Objects []struct {
			Name string `json:"name"`
			Size int64  `json:"size"`
		} `json:"objects"`
	}
	d := json.NewDecoder(f)
	d.DisallowUnknownFields()
	if err := d.Decode(&manifest); err != nil {
		return nil, err
	}
	if err := d.Decode(new(any)); err != io.EOF {
		return nil, fmt.Errorf("object manifest contains trailing data")
	}
	if expected <= 0 || len(manifest.Objects) != expected {
		return nil, fmt.Errorf("object manifest count: got %d, expected %d", len(manifest.Objects), expected)
	}
	seen := make(map[string]bool, expected)
	objects := make(generator.Objects, 0, expected)
	for _, obj := range manifest.Objects {
		if obj.Name == "" || obj.Size <= 0 || seen[obj.Name] {
			return nil, fmt.Errorf("object manifest has an empty, invalid or duplicate object")
		}
		seen[obj.Name] = true
		objects = append(objects, generator.Object{Name: obj.Name, Size: obj.Size})
	}
	return objects, nil
}
