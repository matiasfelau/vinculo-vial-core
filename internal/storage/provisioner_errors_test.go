package storage

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

func TestApply_HeadBucket404SinTipoEsBucketInexistenteYSeCrea(t *testing.T) {
	api := &fakeS3{head: func(context.Context, int) error { return statusError(http.StatusNotFound) }}

	if err := newTestProvisioner(api, testCfg()).Apply(context.Background()); err != nil {
		t.Fatalf("error inesperado: %v", err)
	}
	if api.countOf("CreateBucket") != 1 {
		t.Fatalf("un 404 de HeadBucket significa que el bucket no existe: CreateBucket se llamó %d veces, se esperaba 1", api.countOf("CreateBucket"))
	}
}

func TestApply_HeadBucketConErrorNoClasificadoFallaSinReintentarNiCrear(t *testing.T) {
	api := &fakeS3{head: func(context.Context, int) error { return statusError(http.StatusBadRequest) }}

	err := newTestProvisioner(api, testCfg()).Apply(context.Background())
	if err == nil || !strings.Contains(err.Error(), "consultando el bucket") {
		t.Fatalf("error = %v, se esperaba 'consultando el bucket'", err)
	}
	if api.headCalls != 1 {
		t.Errorf("HeadBucket se llamó %d veces: un 400 no es transitorio y no se reintenta", api.headCalls)
	}
	if api.countOf("CreateBucket") != 0 {
		t.Error("ante un error no clasificado no se debe crear el bucket")
	}
}

func TestApply_CancelarDuranteElBackoffDetieneLaEspera(t *testing.T) {
	api := &fakeS3{head: func(context.Context, int) error { return connectionError() }}
	p := NewProvisioner(api, EvidenceSpec(testCfg()), testCfg(),
		WaitOptions{Timeout: time.Minute, InitialBackoff: time.Minute, MaxBackoff: time.Minute}, discardLogger())

	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(30*time.Millisecond, cancel)

	done := make(chan error, 1)
	go func() { done <- p.Apply(ctx) }()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("error = %v, se esperaba context.Canceled", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("la cancelación del contexto no interrumpió la espera del backoff")
	}
}

func TestApply_ErroresDeCreacionYDeConfiguracionSeEnvuelvenConElPasoQueFallo(t *testing.T) {
	boom := errors.New("fallo de la operación")
	notFound := func(context.Context, int) error { return &types.NotFound{} }

	tests := []struct {
		name string
		api  *fakeS3
		want string
	}{
		{"crear el bucket", &fakeS3{head: notFound, create: func() error { return boom }}, "creando el bucket"},
		{"block public access", &fakeS3{putPAB: func() error { return boom }}, "Block Public Access"},
		{"lifecycle", &fakeS3{putLife: func() error { return boom }}, "lifecycle"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := newTestProvisioner(tt.api, testCfg()).Apply(context.Background())
			if !errors.Is(err, boom) {
				t.Fatalf("el error %v debería envolver la causa original", err)
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Errorf("el error %q debería mencionar %q", err.Error(), tt.want)
			}
		})
	}
}

func TestIsTransient_ClasificaLosFallosPorReintentabilidad(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"deadline excedido", context.DeadlineExceeded, true},
		{"error de red", connectionError(), true},
		{"500", statusError(http.StatusInternalServerError), true},
		{"503", statusError(http.StatusServiceUnavailable), true},
		{"408", statusError(http.StatusRequestTimeout), true},
		{"429", statusError(http.StatusTooManyRequests), true},
		{"400", statusError(http.StatusBadRequest), false},
		{"403", statusError(http.StatusForbidden), false},
		{"error sin respuesta HTTP", errors.New("credenciales ausentes"), false},
		{"permanente aunque envuelva un 503", &permanentError{statusError(http.StatusServiceUnavailable)}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isTransient(tt.err); got != tt.want {
				t.Errorf("isTransient(%v) = %v, se esperaba %v", tt.err, got, tt.want)
			}
		})
	}
}

func TestPermanentError_ExponeMensajeYCausa(t *testing.T) {
	cause := errors.New("causa original")
	err := &permanentError{cause}

	if err.Error() != "causa original" {
		t.Errorf("Error() = %q, se esperaba el mensaje de la causa", err.Error())
	}
	if !errors.Is(err, cause) || errors.Unwrap(err) != cause {
		t.Error("permanentError debe exponer la causa con Unwrap")
	}
}

func TestJitter_AcotaLaEsperaAlBackoff(t *testing.T) {
	if got := jitter(0); got != 0 {
		t.Errorf("jitter(0) = %v, se esperaba 0", got)
	}
	if got := jitter(-time.Second); got != 0 {
		t.Errorf("jitter(negativo) = %v, se esperaba 0", got)
	}
	for range 200 {
		if got := jitter(10 * time.Millisecond); got <= 0 || got > 10*time.Millisecond {
			t.Fatalf("jitter(10ms) = %v, fuera de (0, 10ms]", got)
		}
	}
}

func discardLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }
