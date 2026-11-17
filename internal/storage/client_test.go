package storage

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// isolateAWSEnv evita que el SDK lea credenciales, perfiles o endpoints de la
// máquina que corre las pruebas.
func isolateAWSEnv(t *testing.T) {
	t.Helper()
	dir := t.TempDir()
	t.Setenv("AWS_CONFIG_FILE", filepath.Join(dir, "config"))
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", filepath.Join(dir, "credentials"))
	t.Setenv("AWS_PROFILE", "")
	t.Setenv("AWS_ACCESS_KEY_ID", "AKIATESTTESTTESTTEST")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "secreto-de-prueba")
	t.Setenv("AWS_SESSION_TOKEN", "")
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
}

func TestNewClient_UsaLaRegionConfigurada(t *testing.T) {
	isolateAWSEnv(t)
	cfg := testCfg()
	cfg.Region = "sa-east-1"

	client, err := NewClient(context.Background(), cfg)
	if err != nil {
		t.Fatalf("error inesperado: %v", err)
	}
	if got := client.Options().Region; got != "sa-east-1" {
		t.Errorf("región del cliente = %q, se esperaba sa-east-1", got)
	}
}

func TestNewClient_NoReintentaInternamenteUnaRespuesta5xx(t *testing.T) {
	isolateAWSEnv(t)

	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()
	t.Setenv("AWS_ENDPOINT_URL", srv.URL)

	client, err := NewClient(context.Background(), testCfg())
	if err != nil {
		t.Fatalf("error inesperado: %v", err)
	}

	_, err = client.HeadBucket(context.Background(), &s3.HeadBucketInput{Bucket: ptr(testBucket)})
	if err == nil {
		t.Fatal("se esperaba el error de la respuesta 503")
	}
	if got := hits.Load(); got != 1 {
		t.Errorf("el servidor recibió %d solicitudes: los reintentos solo los hace el Provisioner, el SDK no debe reintentar", got)
	}
}

func TestNewClient_AplicaElTimeoutPorOperacionAlClienteHTTP(t *testing.T) {
	isolateAWSEnv(t)

	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { <-release }))
	defer srv.Close()
	defer close(release)
	t.Setenv("AWS_ENDPOINT_URL", srv.URL)

	cfg := testCfg()
	cfg.Timeout = 50 * time.Millisecond
	client, err := NewClient(context.Background(), cfg)
	if err != nil {
		t.Fatalf("error inesperado: %v", err)
	}

	start := time.Now()
	_, err = client.HeadBucket(context.Background(), &s3.HeadBucketInput{Bucket: ptr(testBucket)})
	if err == nil {
		t.Fatal("se esperaba un timeout contra un servidor que no responde")
	}
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Errorf("la llamada tardó %v: el timeout del cliente HTTP no se aplicó", elapsed)
	}
}

func TestNewClient_FallaSiLaConfiguracionDelSDKEsInvalida(t *testing.T) {
	isolateAWSEnv(t)
	t.Setenv("AWS_CA_BUNDLE", filepath.Join(t.TempDir(), "no-existe.pem"))

	client, err := NewClient(context.Background(), testCfg())
	if err == nil {
		t.Fatalf("se esperaba error con un bundle de CA inexistente, se obtuvo cliente %v", client)
	}
	if !strings.Contains(err.Error(), "configuración del SDK de AWS") {
		t.Errorf("el error %q debería indicar que falló la configuración del SDK", err.Error())
	}
}

func ptr[T any](v T) *T { return &v }
