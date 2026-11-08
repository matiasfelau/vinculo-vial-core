package config

import (
	"strings"
	"testing"
	"time"
)

func setStorageEnv(t *testing.T) {
	t.Helper()
	t.Setenv("S3_REGION", "us-east-1")
	t.Setenv("EVIDENCE_BUCKET", "vinculo-vial-evidence-dev-abc123")
	t.Setenv("AUDIT_BUCKET", "vinculo-vial-audit-dev-abc123")
	t.Setenv("S3_TIMEOUT_MS", "")
	t.Setenv("EVIDENCE_PENDING_DAYS", "")
	t.Setenv("EVIDENCE_QUARANTINE_DAYS", "")
}

func TestLoadStorage_ValoresPorDefectoDelContrato(t *testing.T) {
	setStorageEnv(t)

	cfg, err := LoadStorage()
	if err != nil {
		t.Fatalf("error inesperado: %v", err)
	}

	if cfg.Region != "us-east-1" || cfg.Bucket != "vinculo-vial-evidence-dev-abc123" {
		t.Errorf("región/bucket = %q/%q", cfg.Region, cfg.Bucket)
	}
	if cfg.AuditBucket != "vinculo-vial-audit-dev-abc123" {
		t.Errorf("AuditBucket = %q", cfg.AuditBucket)
	}
	if cfg.Timeout != 5*time.Second {
		t.Errorf("Timeout = %s, se esperaba 5s", cfg.Timeout)
	}
	if cfg.PendingDays != 2 {
		t.Errorf("PendingDays = %d, se esperaba 2", cfg.PendingDays)
	}
	if cfg.QuarantineDays != 15 {
		t.Errorf("QuarantineDays = %d, se esperaba 15", cfg.QuarantineDays)
	}
}

func TestLoadStorage_TomaLosValoresDelEntorno(t *testing.T) {
	setStorageEnv(t)
	t.Setenv("S3_TIMEOUT_MS", "1500")
	t.Setenv("EVIDENCE_PENDING_DAYS", "3")
	t.Setenv("EVIDENCE_QUARANTINE_DAYS", "20")

	cfg, err := LoadStorage()
	if err != nil {
		t.Fatalf("error inesperado: %v", err)
	}

	if cfg.Timeout != 1500*time.Millisecond || cfg.PendingDays != 3 || cfg.QuarantineDays != 20 {
		t.Errorf("cfg = %+v", cfg)
	}
}

func TestLoadStorage_RegionObligatoria(t *testing.T) {
	setStorageEnv(t)
	t.Setenv("S3_REGION", "")

	if _, err := LoadStorage(); err == nil {
		t.Fatal("se esperaba error al no definir S3_REGION")
	}
}

func TestLoadStorage_BucketObligatorio(t *testing.T) {
	setStorageEnv(t)
	t.Setenv("EVIDENCE_BUCKET", "")

	if _, err := LoadStorage(); err == nil {
		t.Fatal("se esperaba error al no definir EVIDENCE_BUCKET")
	}
}

func TestLoadStorage_BucketDeAuditObligatorio(t *testing.T) {
	setStorageEnv(t)
	t.Setenv("AUDIT_BUCKET", "")

	_, err := LoadStorage()
	if err == nil {
		t.Fatal("se esperaba error al no definir AUDIT_BUCKET")
	}
	if !strings.Contains(err.Error(), "AUDIT_BUCKET") {
		t.Errorf("el error %q debería mencionar AUDIT_BUCKET", err.Error())
	}
}

func TestLoadStorage_NombreDeBucketInvalido(t *testing.T) {
	for _, envVar := range []string{"EVIDENCE_BUCKET", "AUDIT_BUCKET"} {
		for _, name := range []string{"ab", "Vinculo-Evidence", "-evidence", "evidence-", "evidence_bucket", "evidence bucket"} {
			t.Run(envVar+"="+name, func(t *testing.T) {
				setStorageEnv(t)
				t.Setenv(envVar, name)

				_, err := LoadStorage()
				if err == nil {
					t.Fatalf("se esperaba error por %s=%q", envVar, name)
				}
				if !strings.Contains(err.Error(), envVar) {
					t.Errorf("el error %q debería mencionar %s", err.Error(), envVar)
				}
			})
		}
	}
}

func TestLoadStorage_LosDosBucketsDebenSerDistintos(t *testing.T) {
	setStorageEnv(t)
	t.Setenv("AUDIT_BUCKET", "vinculo-vial-evidence-dev-abc123")

	if _, err := LoadStorage(); err == nil {
		t.Fatal("se esperaba error: AUDIT_BUCKET igual a EVIDENCE_BUCKET")
	}
}

func TestLoadStorage_ValoresNumericosInvalidos(t *testing.T) {
	for _, name := range []string{"S3_TIMEOUT_MS", "EVIDENCE_PENDING_DAYS", "EVIDENCE_QUARANTINE_DAYS"} {
		for _, raw := range []string{"no-numerico", "0", "-1"} {
			t.Run(name+"="+raw, func(t *testing.T) {
				setStorageEnv(t)
				t.Setenv(name, raw)

				if _, err := LoadStorage(); err == nil {
					t.Fatalf("se esperaba error por %s=%q", name, raw)
				}
			})
		}
	}
}
