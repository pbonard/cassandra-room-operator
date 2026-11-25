// Package samples checks that the samples are valid objects: the API
// server accepts them (the CRDs' schemas).
package samples

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/yaml"

	"github.com/pbonard/cassandra-room-operator/operator/internal/testenv"
)

func TestSamples(t *testing.T) {
	env, s := testenv.StartEnv(t)
	c, err := client.New(env.Config, client.Options{Scheme: s})
	require.NoError(t, err)
	files, err := filepath.Glob("../../config/samples/*.yaml")
	require.NoError(t, err)
	require.NotEmpty(t, files)
	for _, f := range files {
		data, err := os.ReadFile(f)
		require.NoError(t, err)
		for _, doc := range strings.Split(string(data), "\n---\n") {
			obj := &unstructured.Unstructured{}
			require.NoError(t, yaml.Unmarshal([]byte(doc), &obj.Object), f)
			obj.SetNamespace("default")
			require.NoError(t, c.Create(context.Background(), obj), "%s: %s %s", f, obj.GetKind(), obj.GetName())
		}
	}
}
