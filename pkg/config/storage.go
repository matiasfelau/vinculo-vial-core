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

// StorageConfig contiene los parámetros del bucket de evidencia que este
// job declara. Las credenciales AWS no forman parte de la configuración: las
// resuelve la cadena estándar del SDK (variables AWS_ACCESS_KEY_ID y
// AWS_SECRET_ACCESS_KEY en local, rol IAM en producción).
type StorageConfig struct {
	Region         string
	Bucket         string
	Timeout        time.Duration
	PendingDays    int32
	QuarantineDays int32
}

// LoadStorage lee StorageConfig desde el entorno. Se carga aparte de Load
// porque solo cmd/core configura el bucket: cmd/reset y la prueba de humo no
// necesitan (ni deben exigir) variables de S3. EVIDENCE_BUCKET y S3_REGION
// son obligatorias y no tienen valor por defecto; el resto toma los valores
// del contrato.
func LoadStorage() (StorageConfig, error) {
	region := os.Getenv("S3_REGION")
	if region == "" {
		return StorageConfig{}, errors.New("config: la variable S3_REGION es obligatoria")
	}

	bucket := os.Getenv("EVIDENCE_BUCKET")
	if bucket == "" {
		return StorageConfig{}, errors.New("config: la variable EVIDENCE_BUCKET es obligatoria")
	}
	if !bucketNamePattern.MatchString(bucket) {
		return StorageConfig{}, errors.New("config: EVIDENCE_BUCKET no es un nombre de bucket de S3 válido " +
			"(3 a 63 caracteres: minúsculas, dígitos, puntos y guiones)")
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
		Timeout:        time.Duration(timeoutMs) * time.Millisecond,
		PendingDays:    pendingDays,
		QuarantineDays: quarantineDays,
	}, nil
}

func parsePositiveInt32(name, raw string) (int32, error) {
	v, err := strconv.ParseInt(raw, 10, 32)
	if err != nil || v <= 0 {
		return 0, fmt.Errorf("config: %s debe ser un entero positivo, se recibió %q", name, raw)
	}
	return int32(v), nil
}
