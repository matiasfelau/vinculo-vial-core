package verify

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/vinculo-vial/core-service/internal/storage"
)

// validAuditBucket es un bucket de Audit correcto: con Object Lock sin
// retención por defecto, versionado, privado, cifrado y con su lifecycle.
func validAuditBucket() *fakeBucket {
	return &fakeBucket{
		pab:        &s3.GetPublicAccessBlockOutput{PublicAccessBlockConfiguration: storage.PublicAccessBlockConfig()},
		enc:        &s3.GetBucketEncryptionOutput{ServerSideEncryptionConfiguration: storage.EncryptionConfig()},
		versioning: &s3.GetBucketVersioningOutput{Status: types.BucketVersioningStatusEnabled},
		lifecycle:  &s3.GetBucketLifecycleConfigurationOutput{Rules: storage.AuditLifecycleRules()},
		lock: &s3.GetObjectLockConfigurationOutput{
			ObjectLockConfiguration: &types.ObjectLockConfiguration{ObjectLockEnabled: types.ObjectLockEnabledEnabled},
		},
	}
}

func TestAuditBucketVerify_ConfiguracionCorrecta(t *testing.T) {
	cfg := bucketTestConfig()
	f := validAuditBucket()

	if err := NewBucketVerifier(f, storage.AuditSpec(cfg), cfg).Verify(context.Background()); err != nil {
		t.Fatalf("error inesperado: %v", err)
	}
	if !contains(f.calls, "GetObjectLockConfiguration") {
		t.Errorf("el bucket de Audit debe consultar Object Lock, llamadas: %v", f.calls)
	}
}

func TestAuditBucketVerify_SinObjectLockEsError(t *testing.T) {
	cfg := bucketTestConfig()
	f := validAuditBucket()
	f.lock, f.lockErr = nil, apiError("ObjectLockConfigurationNotFoundError")

	mustFailWithSpec(t, f, storage.AuditSpec(cfg), cfg, "sin Object Lock", "no se puede habilitar en un bucket existente", "no lo borra ni lo recrea", "AUDIT_BUCKET")
}

func TestAuditBucketVerify_ObjectLockDeshabilitadoEsError(t *testing.T) {
	cfg := bucketTestConfig()

	cases := map[string]*s3.GetObjectLockConfigurationOutput{
		"configuración vacía": {},
		"estado vacío":        {ObjectLockConfiguration: &types.ObjectLockConfiguration{}},
	}
	for name, lock := range cases {
		t.Run(name, func(t *testing.T) {
			f := validAuditBucket()
			f.lock = lock

			mustFailWithSpec(t, f, storage.AuditSpec(cfg), cfg, "sin Object Lock")
		})
	}
}

func TestAuditBucketVerify_RetencionPorDefectoEsError(t *testing.T) {
	cfg := bucketTestConfig()
	f := validAuditBucket()
	f.lock = &s3.GetObjectLockConfigurationOutput{ObjectLockConfiguration: &types.ObjectLockConfiguration{
		ObjectLockEnabled: types.ObjectLockEnabledEnabled,
		Rule: &types.ObjectLockRule{DefaultRetention: &types.DefaultRetention{
			Mode: types.ObjectLockRetentionModeGovernance,
			Days: aws.Int32(1),
		}},
	}}

	mustFailWithSpec(t, f, storage.AuditSpec(cfg), cfg, "retención por defecto")
}

func TestAuditBucketVerify_SinVersionadoEsError(t *testing.T) {
	cfg := bucketTestConfig()

	for _, status := range []types.BucketVersioningStatus{"", types.BucketVersioningStatusSuspended} {
		f := validAuditBucket()
		f.versioning = &s3.GetBucketVersioningOutput{Status: status}

		mustFailWithSpec(t, f, storage.AuditSpec(cfg), cfg, "versionado no está habilitado")
	}
}

func TestAuditBucketVerify_LifecycleDeEvidenciaEsError(t *testing.T) {
	cfg := bucketTestConfig()
	f := validAuditBucket()
	f.lifecycle = &s3.GetBucketLifecycleConfigurationOutput{Rules: storage.LifecycleRules(cfg)}

	mustFailWithSpec(t, f, storage.AuditSpec(cfg), cfg, "lifecycle difiere", storage.RuleAuditNoncurrentID)
}

func TestAuditBucketVerify_LifecycleConDiasDeVersionesNoVigentesDistintos(t *testing.T) {
	cfg := bucketTestConfig()
	f := validAuditBucket()
	rules := storage.AuditLifecycleRules()
	rules[0].NoncurrentVersionExpiration.NoncurrentDays = aws.Int32(30)
	f.lifecycle = &s3.GetBucketLifecycleConfigurationOutput{Rules: rules}

	mustFailWithSpec(t, f, storage.AuditSpec(cfg), cfg, "lifecycle difiere", "noncurrent=1d", "noncurrent=30d")
}

func TestAuditBucketVerify_LifecycleSinLimpiezaDeDeleteMarkers(t *testing.T) {
	cfg := bucketTestConfig()
	f := validAuditBucket()
	rules := storage.AuditLifecycleRules()
	rules[0].Expiration = nil
	f.lifecycle = &s3.GetBucketLifecycleConfigurationOutput{Rules: rules}

	mustFailWithSpec(t, f, storage.AuditSpec(cfg), cfg, "lifecycle difiere", "delete-marker=true")
}

func TestAuditBucketVerify_LifecycleQueExpiraObjetosVigentesEsError(t *testing.T) {
	cfg := bucketTestConfig()
	f := validAuditBucket()
	rules := storage.AuditLifecycleRules()
	rules[0].Expiration = &types.LifecycleExpiration{Days: aws.Int32(1)}
	f.lifecycle = &s3.GetBucketLifecycleConfigurationOutput{Rules: rules}

	mustFailWithSpec(t, f, storage.AuditSpec(cfg), cfg, "lifecycle difiere", "expira=1d")
}

func TestAuditBucketVerify_PublicAccessBlockYCifradoSeExigen(t *testing.T) {
	cfg := bucketTestConfig()

	f := validAuditBucket()
	f.pab, f.pabErr = nil, apiError("NoSuchPublicAccessBlockConfiguration")
	mustFailWithSpec(t, f, storage.AuditSpec(cfg), cfg, "Block Public Access no está configurado")

	f = validAuditBucket()
	f.enc, f.encErr = nil, apiError("ServerSideEncryptionConfigurationNotFoundError")
	mustFailWithSpec(t, f, storage.AuditSpec(cfg), cfg, "cifrado por defecto no está configurado")
}

func TestAuditBucketVerify_ErrorDeConsultaDeObjectLockEsFailClosed(t *testing.T) {
	cfg := bucketTestConfig()
	boom := errors.New("fallo de red")
	f := validAuditBucket()
	f.lock, f.lockErr = nil, boom

	err := NewBucketVerifier(f, storage.AuditSpec(cfg), cfg).Verify(context.Background())
	if !errors.Is(err, boom) {
		t.Fatalf("error = %v, se esperaba que envolviera la falla de consulta", err)
	}
}

func TestAuditBucketVerify_ReportaTodosLosProblemasJuntos(t *testing.T) {
	cfg := bucketTestConfig()
	f := validAuditBucket()
	f.lock, f.lockErr = nil, apiError("ObjectLockConfigurationNotFoundError")
	f.versioning = &s3.GetBucketVersioningOutput{}
	f.lifecycle, f.lifeErr = nil, apiError("NoSuchLifecycleConfiguration")

	mustFailWithSpec(t, f, storage.AuditSpec(cfg), cfg, "Object Lock", "versionado", "lifecycle")
}

func TestBucketVerify_EvidenciaNoConsultaObjectLock(t *testing.T) {
	cfg := bucketTestConfig()
	f := validBucket(cfg)

	if err := NewBucketVerifier(f, storage.EvidenceSpec(cfg), cfg).Verify(context.Background()); err != nil {
		t.Fatalf("error inesperado: %v", err)
	}
	if contains(f.calls, "GetObjectLockConfiguration") {
		t.Errorf("el bucket de evidencia no lleva Object Lock y no debe consultarlo, llamadas: %v", f.calls)
	}
}

func TestBucketVerify_BucketDeAuditInexistenteFallaSinConsultarLaConfiguracion(t *testing.T) {
	cfg := bucketTestConfig()
	f := validAuditBucket()
	f.head = &types.NotFound{}

	mustFailWithSpec(t, f, storage.AuditSpec(cfg), cfg, "no es accesible", "vinculo-vial-audit-test")
	if len(f.calls) != 1 {
		t.Fatalf("tras un HeadBucket fallido no se debe seguir consultando, llamadas: %v", f.calls)
	}
}

func contains(calls []string, op string) bool {
	for _, c := range calls {
		if c == op {
			return true
		}
	}
	return false
}
