package config

import (
	"errors"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"time"
)

const (
	defaultS3TimeoutMs    = "5000"
	defaultPendingDays    = "2"
	defaultQuarantineDays = "15"
)

// bucketNamePattern reproduce las reglas de nombre de bucket de S3 de uso
// general: 3 a 63 caracteres, minúsculas, dígitos, puntos y guiones, y
// comienza y termina con letra o dígito.
var bucketNamePattern = regexp.MustCompile(`^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$`)

// StorageConfig contiene los parámetros de los buckets que este job declara:
// el de evidencia y, desde US-11, el de Audit. Las credenciales AWS no forman
// parte de la configuración: las resuelve la cadena estándar del SDK
// (variables AWS_ACCESS_KEY_ID y AWS_SECRET_ACCESS_KEY en local, rol IAM en
// producción).
type StorageConfig struct {
	Region string
	// Bucket es el bucket de evidencia (EVIDENCE_BUCKET).
	Bucket string
	// AuditBucket es el bucket de Audit, con S3 Object Lock (AUDIT_BUCKET).
	// Core no fija su retención: la fija Audit en cada objeto.
	AuditBucket    string
	Timeout        time.Duration
	PendingDays    int32
	QuarantineDays int32
}

// LoadStorage lee StorageConfig desde el entorno. Se carga aparte de Load
// porque solo cmd/core configura los buckets: cmd/reset y la prueba de humo
// no necesitan (ni deben exigir) variables de S3. S3_REGION, EVIDENCE_BUCKET
// y AUDIT_BUCKET son obligatorias y no tienen valor por defecto; el resto
// toma los valores del contrato.
func LoadStorage() (StorageConfig, error) {
	region := os.Getenv("S3_REGION")
	if region == "" {
		return StorageConfig{}, errors.New("config: la variable S3_REGION es obligatoria")
	}

	bucket, err := loadBucketName("EVIDENCE_BUCKET")
	if err != nil {
		return StorageConfig{}, err
	}
	auditBucket, err := loadBucketName("AUDIT_BUCKET")
	if err != nil {
		return StorageConfig{}, err
	}
	if auditBucket == bucket {
		return StorageConfig{}, errors.New("config: AUDIT_BUCKET y EVIDENCE_BUCKET deben ser buckets distintos: " +
			"el de Audit tiene Object Lock y el de evidencia no")
	}

	timeoutMs, err := parsePositiveInt32("S3_TIMEOUT_MS", getEnvOrDefault("S3_TIMEOUT_MS", defaultS3TimeoutMs))
	if err != nil {
		return StorageConfig{}, err
	}
	pendingDays, err := parsePositiveInt32("EVIDENCE_PENDING_DAYS", getEnvOrDefault("EVIDENCE_PENDING_DAYS", defaultPendingDays))
	if err != nil {
		return StorageConfig{}, err
	}
	quarantineDays, err := parsePositiveInt32("EVIDENCE_QUARANTINE_DAYS", getEnvOrDefault("EVIDENCE_QUARANTINE_DAYS", defaultQuarantineDays))
	if err != nil {
		return StorageConfig{}, err
	}

	return StorageConfig{
		Region:         region,
		Bucket:         bucket,
		AuditBucket:    auditBucket,
		Timeout:        time.Duration(timeoutMs) * time.Millisecond,
		PendingDays:    pendingDays,
		QuarantineDays: quarantineDays,
	}, nil
}

// loadBucketName lee una variable con el nombre de un bucket: es obligatoria
// y debe cumplir las reglas de nombre de S3.
func loadBucketName(name string) (string, error) {
	bucket := os.Getenv(name)
	if bucket == "" {
		return "", fmt.Errorf("config: la variable %s es obligatoria", name)
	}
	if !bucketNamePattern.MatchString(bucket) {
		return "", fmt.Errorf("config: %s no es un nombre de bucket de S3 válido "+
			"(3 a 63 caracteres: minúsculas, dígitos, puntos y guiones)", name)
	}
	return bucket, nil
}

func parsePositiveInt32(name, raw string) (int32, error) {
	v, err := strconv.ParseInt(raw, 10, 32)
	if err != nil || v <= 0 {
		return 0, fmt.Errorf("config: %s debe ser un entero positivo, se recibió %q", name, raw)
	}
	return int32(v), nil
}
