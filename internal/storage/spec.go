// Package storage declara el bucket de evidencia en AWS S3: existencia,
// acceso privado, cifrado por defecto y reglas de lifecycle por tag. Core no
// lee ni escribe objetos: solo configura el bucket.
package storage

import (
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/vinculo-vial/core-service/pkg/config"
)

const (
	// StatusTagKey es la clave del tag que Alert asigna a los originales
	// cifrados y que selecciona la regla de lifecycle aplicable.
	StatusTagKey = "status"

	// StatusTagPending marca los originales que todavía no recibieron
	// veredicto.
	StatusTagPending = "pending"

	// StatusTagQuarantine marca los originales de alertas RED (ventana de
	// apelación).
	StatusTagQuarantine = "quarantine"

	// RulePendingID identifica la regla de expiración de los originales
	// pendientes.
	RulePendingID = "evidence-pending-expiration"

	// RuleQuarantineID identifica la regla de expiración de los originales en
	// cuarentena.
	RuleQuarantineID = "evidence-quarantine-expiration"

	// regionUSEast1 es la región por defecto de S3: CreateBucket rechaza un
	// LocationConstraint explícito para ella.
	regionUSEast1 = "us-east-1"
)

// LifecycleRules devuelve el conjunto completo de reglas de lifecycle del
// bucket, parametrizado por los días del entorno. Se aplica entero en cada
// corrida (reemplazo, no merge) y es también lo que la verificación espera
// encontrar.
func LifecycleRules(cfg config.StorageConfig) []types.LifecycleRule {
	return []types.LifecycleRule{
		expirationByTag(RulePendingID, StatusTagPending, cfg.PendingDays),
		expirationByTag(RuleQuarantineID, StatusTagQuarantine, cfg.QuarantineDays),
	}
}

func expirationByTag(id, statusValue string, days int32) types.LifecycleRule {
	return types.LifecycleRule{
		ID:     aws.String(id),
		Status: types.ExpirationStatusEnabled,
		Filter: &types.LifecycleRuleFilter{
			Tag: &types.Tag{Key: aws.String(StatusTagKey), Value: aws.String(statusValue)},
		},
		Expiration: &types.LifecycleExpiration{Days: aws.Int32(days)},
	}
}

// PublicAccessBlockConfig devuelve el Block Public Access esperado: los
// cuatro indicadores en true.
func PublicAccessBlockConfig() *types.PublicAccessBlockConfiguration {
	return &types.PublicAccessBlockConfiguration{
		BlockPublicAcls:       aws.Bool(true),
		BlockPublicPolicy:     aws.Bool(true),
		IgnorePublicAcls:      aws.Bool(true),
		RestrictPublicBuckets: aws.Bool(true),
	}
}

// EncryptionConfig devuelve el cifrado por defecto esperado: SSE-S3
// (AES256).
func EncryptionConfig() *types.ServerSideEncryptionConfiguration {
	return &types.ServerSideEncryptionConfiguration{
		Rules: []types.ServerSideEncryptionRule{
			{
				ApplyServerSideEncryptionByDefault: &types.ServerSideEncryptionByDefault{
					SSEAlgorithm: types.ServerSideEncryptionAes256,
				},
			},
		},
	}
}
