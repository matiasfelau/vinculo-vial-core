//go:build aws

package storage_test

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/vinculo-vial/core-service/internal/storage"
	"github.com/vinculo-vial/core-service/internal/verify"
	"github.com/vinculo-vial/core-service/pkg/config"
)

const (
	integrationTimeout = 2 * time.Minute
	integrationWait    = 10 * time.Second
	integrationBackoff = 500 * time.Millisecond
	integrationMaxWait = 2 * time.Second
)

// TestAWS_DosCorridasSeguidasYVerificacionCompleta ejercita el aprovisionador
// y el verificador contra los buckets de desarrollo reales de AWS S3 (el de
// evidencia y el de Audit). Es opt-in (tag "aws"): requiere S3_REGION,
// EVIDENCE_BUCKET y AUDIT_BUCKET, y credenciales en la cadena estándar del
// SDK (AWS_ACCESS_KEY_ID y AWS_SECRET_ACCESS_KEY).
//
// Aplica la configuración dos veces seguidas sin cambios y verifica cada
// bucket tras cada corrida: la segunda no debe crearlo ni alterar nada, y la
// verificación completa (Block Public Access, cifrado, versionado, lifecycle
// y, en el de Audit, Object Lock) debe quedar en verde.
func TestAWS_DosCorridasSeguidasYVerificacionCompleta(t *testing.T) {
	cfg, err := config.LoadStorage()
	if err != nil {
		t.Fatalf("configuración del bucket de desarrollo: %v", err)
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	ctx, cancel := context.WithTimeout(context.Background(), integrationTimeout)
	defer cancel()

	client, err := storage.NewClient(ctx, cfg)
	if err != nil {
		t.Fatalf("creando el cliente de S3: %v", err)
	}

	wait := storage.WaitOptions{Timeout: integrationWait, InitialBackoff: integrationBackoff, MaxBackoff: integrationMaxWait}

	for _, spec := range []storage.BucketSpec{storage.EvidenceSpec(cfg), storage.AuditSpec(cfg)} {
		provisioner := storage.NewProvisioner(client, spec, cfg, wait, logger)
		verifier := verify.NewBucketVerifier(client, spec, cfg)

		for run := 1; run <= 2; run++ {
			if err := provisioner.Apply(ctx); err != nil {
				t.Fatalf("bucket %s, corrida %d: declarando el bucket: %v", spec.Label, run, err)
			}
			if err := verifier.Verify(ctx); err != nil {
				t.Fatalf("bucket %s, corrida %d: verificando el bucket: %v", spec.Label, run, err)
			}
		}
	}
}
