// Package storage declara en AWS S3 el bucket de evidencia (existencia,
// acceso privado, cifrado por defecto y reglas de lifecycle por tag) y, desde
// US-11, el bucket de Audit (lo mismo, más versionado y Object Lock). Core no
// lee ni escribe objetos: solo configura los buckets.
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

	// RuleAuditNoncurrentID identifica la regla del bucket de Audit que limpia
	// las versiones no vigentes y los delete markers huérfanos.
	RuleAuditNoncurrentID = "audit-noncurrent-cleanup"

	// auditNoncurrentDays es el plazo en que expiran las versiones no
	// vigentes del bucket de Audit. Limpian lo que Audit borra: S3 respeta la
	// retención vigente de cada versión, así que no adelanta ningún borrado.
	auditNoncurrentDays = 1

	// regionUSEast1 es la región por defecto de S3: CreateBucket rechaza un
	// LocationConstraint explícito para ella.
	regionUSEast1 = "us-east-1"
)

// BucketSpec describe un bucket que Core declara. El aprovisionador y el
// verificador la recorren de forma genérica: agregar otro bucket es sumar una
// especificación, no escribir código de declaración.
type BucketSpec struct {
	// Name es el nombre global del bucket en S3.
	Name string

	// Label nombra al bucket en logs y errores ("de evidencia", "de Audit").
	Label string

	// EnvVar es la variable que define Name, para que los errores indiquen
	// qué cambiar.
	EnvVar string

	// ObjectLock indica que el bucket nace con S3 Object Lock, lo que activa
	// el versionado. Solo se puede habilitar al crearlo: en uno existente sin
	// Object Lock la verificación falla, y Core no lo borra ni lo recrea. Sin
	// ObjectLock el bucket debe estar sin versionar.
	//
	// Core nunca fija una retención por defecto: la fija el dueño del bucket
	// en cada objeto.
	ObjectLock bool

	// Rules es el conjunto completo de reglas de lifecycle. Se aplica entero
	// en cada corrida (reemplazo, no merge) y es lo que la verificación
	// espera encontrar.
	Rules []types.LifecycleRule
}

// EvidenceSpec devuelve la especificación del bucket de evidencia: sin
// versionado y con lifecycle por tag parametrizado por los días del entorno.
func EvidenceSpec(cfg config.StorageConfig) BucketSpec {
	return BucketSpec{
		Name:   cfg.Bucket,
		Label:  "de evidencia",
		EnvVar: "EVIDENCE_BUCKET",
		Rules:  LifecycleRules(cfg),
	}
}

// AuditSpec devuelve la especificación del bucket de Audit: privado, SSE-S3,
// versionado y con Object Lock, sin retención por defecto.
func AuditSpec(cfg config.StorageConfig) BucketSpec {
	return BucketSpec{
		Name:       cfg.AuditBucket,
		Label:      "de Audit",
		EnvVar:     "AUDIT_BUCKET",
		ObjectLock: true,
		Rules:      AuditLifecycleRules(),
	}
}

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

// AuditLifecycleRules devuelve las reglas de lifecycle del bucket de Audit:
// una única regla sobre todo el bucket que expira las versiones no vigentes al
// día y elimina los delete markers sin versiones debajo. Audit borra con
// versionado, así que sin esta regla cada borrado dejaría versiones y markers
// acumulándose; con retención vigente S3 no las elimina antes de tiempo.
func AuditLifecycleRules() []types.LifecycleRule {
	return []types.LifecycleRule{
		{
			ID:     aws.String(RuleAuditNoncurrentID),
			Status: types.ExpirationStatusEnabled,
			Filter: &types.LifecycleRuleFilter{Prefix: aws.String("")},
			NoncurrentVersionExpiration: &types.NoncurrentVersionExpiration{
				NoncurrentDays: aws.Int32(auditNoncurrentDays),
			},
			Expiration: &types.LifecycleExpiration{ExpiredObjectDeleteMarker: aws.Bool(true)},
		},
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
