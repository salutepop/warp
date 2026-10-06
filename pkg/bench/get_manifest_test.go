// Copyright 2026. Licensed under the GNU Affero General Public License v3.0 or later.
package bench

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
)

func TestGetManifestRejectsInvalidFixture(t *testing.T) {
	for _, input := range []string{
		`{"objects":[]}`,
		`{"objects":[{"name":"a","size":1},{"name":"a","size":1}]}`,
		`{"objects":[{"name":"","size":1}]}`,
		`{"objects":[{"name":"a","size":0}]}`,
		`{"objects":[{"name":"a","size":1}],"unknown":true}`,
		`{"objects":[{"name":"a","size":1}]} {}`,
	} {
		p := filepath.Join(t.TempDir(), "objects.json")
		if err := os.WriteFile(p, []byte(input), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := loadGetObjects(p, 1); err == nil {
			t.Fatalf("accepted invalid manifest %s", input)
		}
	}
}

func TestGetOnceConsumesEveryObjectWithoutListingOrReuse(t *testing.T) {
	for _, concurrency := range []int{1, 7, 128} {
		t.Run(fmt.Sprint(concurrency), func(t *testing.T) {
			var mu sync.Mutex
			seen := make(map[string]int)
			var failures []string
			payload := bytes.Repeat([]byte("z"), 4096)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				seen[r.URL.Path]++
				if r.Method != http.MethodGet || r.URL.RawQuery != "" {
					failures = append(failures, r.Method+" "+r.URL.String())
				}
				mu.Unlock()
				w.Header().Set("Content-Length", fmt.Sprint(len(payload)))
				_, _ = w.Write(payload)
			}))
			defer server.Close()
			client, err := minio.New(strings.TrimPrefix(server.URL, "http://"), &minio.Options{
				Region: "us-east-1", BucketLookup: minio.BucketLookupPath,
			})
			if err != nil {
				t.Fatal(err)
			}
			objects := make([]map[string]any, 97)
			for i := range objects {
				objects[i] = map[string]any{"name": fmt.Sprintf("obj-%03d", i), "size": len(payload)}
			}
			data, err := json.Marshal(map[string]any{"objects": objects})
			if err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(t.TempDir(), "objects.json")
			if err := os.WriteFile(path, data, 0o600); err != nil {
				t.Fatal(err)
			}
			collector, result := NewOpsCollector()
			g := &Get{Once: true, ObjectsFile: path, CreateObjects: len(objects), Common: Common{
				Concurrency: concurrency, Bucket: "fixture", Collector: collector,
				Client: func() (*minio.Client, func()) { return client, func() {} },
				Error: func(args ...any) {
					mu.Lock()
					defer mu.Unlock()
					failures = append(failures, fmt.Sprint(args...))
				},
			}}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := g.Prepare(ctx); err != nil {
				t.Fatal(err)
			}
			mu.Lock()
			preparedRequests := len(seen)
			mu.Unlock()
			if preparedRequests != 0 {
				t.Fatal("preparation unexpectedly accessed the server")
			}
			start := make(chan struct{})
			close(start)
			if err := g.Start(ctx, start); err != nil {
				t.Fatal(err)
			}
			g.Cleanup(ctx)
			ops := result()
			if len(ops) != len(objects) {
				t.Fatalf("got %d operations; expected %d", len(ops), len(objects))
			}
			for _, op := range ops {
				if op.Err != "" || op.Size != int64(len(payload)) {
					t.Fatalf("invalid operation: %+v", op)
				}
			}
			mu.Lock()
			defer mu.Unlock()
			if len(failures) != 0 || len(seen) != len(objects) {
				t.Fatalf("requests: %d, errors: %v", len(seen), failures)
			}
			for path, count := range seen {
				if count != 1 {
					t.Fatalf("object %s requested %d times", path, count)
				}
			}
		})
	}
}
