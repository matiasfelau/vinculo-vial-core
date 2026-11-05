package storage

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

func TestAuditSpec_Declaracion(t *testing.T) {
	spec := AuditSpec(testCfg())

	if spec.Name != testAuditBucket {
		t.Errorf("Name = %q, se esperaba %q", spec.Name, testAuditBucket)
	}
	if spec.EnvVar != "AUDIT_BUCKET" {
		t.Errorf("EnvVar = %q, se esperaba AUDIT_BUCKET", spec.EnvVar)
	}
	if !spec.ObjectLock {
		t.Error("el bucket de Audit debe declararse con Object Lock")
	}
}

func TestEvidenceSpec_NoCambia(t *testing.T) {
	cfg := testCfg()
	spec := EvidenceSpec(cfg)

	if spec.Name != testBucket || spec.EnvVar != "EVIDENCE_BUCKET" {
		t.Errorf("spec = %q/%q", spec.Name, spec.EnvVar)
	}
	if spec.ObjectLock {
		t.Error("el bucket de evidencia no lleva Object Lock")
	}
	if !reflect.DeepEqual(spec.Rules, LifecycleRules(cfg)) {
		t.Error("el lifecycle del bucket de evidencia debe seguir siendo el de siempre")
	}
}

func TestAuditLifecycleRules_UnaReglaQueLimpiaLoQueAuditBorra(t *testing.T) {
	rules := AuditLifecycleRules()
	if len(rules) != 1 {
		t.Fatalf("el lifecycle de Audit tiene %d reglas, se esperaba 1", len(rules))
	}
	r := rules[0]

	if aws.ToString(r.ID) != RuleAuditNoncurrentID || r.Status != types.ExpirationStatusEnabled {
		t.Errorf("regla %q con estado %s", aws.ToString(r.ID), r.Status)
	}
	if r.Filter == nil || aws.ToString(r.Filter.Prefix) != "" || r.Filter.Tag != nil || r.Filter.And != nil {
		t.Errorf("la regla debe alcanzar a todo el bucket, filtro = %+v", r.Filter)
	}
	if r.NoncurrentVersionExpiration == nil || aws.ToInt32(r.NoncurrentVersionExpiration.NoncurrentDays) != 1 {
		t.Errorf("NoncurrentVersionExpiration = %+v, se esperaban 1 día", r.NoncurrentVersionExpiration)
	}
	if r.Expiration == nil || !aws.ToBool(r.Expiration.ExpiredObjectDeleteMarker) {
		t.Errorf("Expiration = %+v, se esperaba ExpiredObjectDeleteMarker=true", r.Expiration)
	}
	if r.Expiration != nil && (r.Expiration.Days != nil || r.Expiration.Date != nil) {
		t.Error("la regla no debe expirar objetos vigentes: ExpiredObjectDeleteMarker no se combina con Days ni Date")
	}
}

func TestApply_BucketDeAuditSeCreaConObjectLockSinRetencionPorDefecto(t *testing.T) {
	cfg := testCfg()
	api := &fakeS3{head: func(context.Context, int) error { return &types.NotFound{} }}

	if err := newSpecProvisioner(api, AuditSpec(cfg), cfg).Apply(context.Background()); err != nil {
		t.Fatalf("error inesperado: %v", err)
	}

	want := []string{"HeadBucket", "CreateBucket", "PutPublicAccessBlock", "PutBucketEncryption", "PutBucketLifecycleConfiguration"}
	if got := api.ops(); !reflect.DeepEqual(got, want) {
		t.Fatalf("operaciones = %v, se esperaba %v: Core no debe fijar retención ni versionado por su cuenta", got, want)
	}

	create := api.lastInput(t, "CreateBucket").(*s3.CreateBucketInput)
	if aws.ToString(create.Bucket) != testAuditBucket {
		t.Errorf("bucket creado = %q, se esperaba %q", aws.ToString(create.Bucket), testAuditBucket)
	}
	if !aws.ToBool(create.ObjectLockEnabledForBucket) {
		t.Error("CreateBucket debe enviar ObjectLockEnabledForBucket=true")
	}
}

func TestApply_BucketDeEvidenciaSeCreaSinObjectLock(t *testing.T) {
	api := &fakeS3{head: func(context.Context, int) error { return &types.NotFound{} }}

	if err := newTestProvisioner(api, testCfg()).Apply(context.Background()); err != nil {
		t.Fatalf("error inesperado: %v", err)
	}

	create := api.lastInput(t, "CreateBucket").(*s3.CreateBucketInput)
	if create.ObjectLockEnabledForBucket != nil {
		t.Errorf("el bucket de evidencia no lleva Object Lock, se envió %v", aws.ToBool(create.ObjectLockEnabledForBucket))
	}
}

func TestApply_BucketDeAuditAplicaSuLifecycleCompleto(t *testing.T) {
	cfg := testCfg()
	api := &fakeS3{}

	if err := newSpecProvisioner(api, AuditSpec(cfg), cfg).Apply(context.Background()); err != nil {
		t.Fatalf("error inesperado: %v", err)
	}

	in := api.lastInput(t, "PutBucketLifecycleConfiguration").(*s3.PutBucketLifecycleConfigurationInput)
	if aws.ToString(in.Bucket) != testAuditBucket {
		t.Errorf("lifecycle aplicado a %q, se esperaba %q", aws.ToString(in.Bucket), testAuditBucket)
	}
	if !reflect.DeepEqual(in.LifecycleConfiguration.Rules, AuditLifecycleRules()) {
		t.Errorf("reglas = %+v, se esperaba el lifecycle de Audit", in.LifecycleConfiguration.Rules)
	}
}

func TestApply_BucketDeAuditExistenteNoSeRecreaYSeReaplicaLaConfiguracion(t *testing.T) {
	cfg := testCfg()
	api := &fakeS3{}

	if err := newSpecProvisioner(api, AuditSpec(cfg), cfg).Apply(context.Background()); err != nil {
		t.Fatalf("error inesperado: %v", err)
	}

	if n := api.countOf("CreateBucket"); n != 0 {
		t.Fatalf("CreateBucket se llamó %d veces con el bucket existente: Core no recrea buckets", n)
	}
	for _, op := range []string{"PutPublicAccessBlock", "PutBucketEncryption", "PutBucketLifecycleConfiguration"} {
		if api.countOf(op) != 1 {
			t.Errorf("%s debe reaplicarse sobre un bucket existente", op)
		}
	}
}

func TestApply_BucketDeAuditAplicaAccesoPrivadoYCifradoSSES3(t *testing.T) {
	cfg := testCfg()
	api := &fakeS3{}

	if err := newSpecProvisioner(api, AuditSpec(cfg), cfg).Apply(context.Background()); err != nil {
		t.Fatalf("error inesperado: %v", err)
	}

	pab := api.lastInput(t, "PutPublicAccessBlock").(*s3.PutPublicAccessBlockInput).PublicAccessBlockConfiguration
	if !reflect.DeepEqual(pab, PublicAccessBlockConfig()) {
		t.Errorf("Block Public Access = %+v, se esperaban los cuatro indicadores en true", pab)
	}
	enc := api.lastInput(t, "PutBucketEncryption").(*s3.PutBucketEncryptionInput).ServerSideEncryptionConfiguration
	if !reflect.DeepEqual(enc, EncryptionConfig()) {
		t.Errorf("cifrado = %+v, se esperaba SSE-S3", enc)
	}
}

func TestApply_NombreDeBucketDeAuditTomadoPorOtraCuentaIndicaAuditBucket(t *testing.T) {
	cfg := testCfg()
	api := &fakeS3{
		head:   func(context.Context, int) error { return &types.NotFound{} },
		create: func() error { return &types.BucketAlreadyExists{} },
	}

	err := newSpecProvisioner(api, AuditSpec(cfg), cfg).Apply(context.Background())
	if err == nil {
		t.Fatal("se esperaba error: el nombre del bucket pertenece a otra cuenta")
	}
	for _, want := range []string{testAuditBucket, "otra cuenta", "AUDIT_BUCKET"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("el error %q debería mencionar %q", err.Error(), want)
		}
	}
	if strings.Contains(err.Error(), "EVIDENCE_BUCKET") {
		t.Errorf("el error %q no debería mandar a cambiar EVIDENCE_BUCKET", err.Error())
	}
}

func TestApply_BucketDeAuditEsIdempotente(t *testing.T) {
	cfg := testCfg()

	first := &fakeS3{head: func(context.Context, int) error { return &types.NotFound{} }}
	if err := newSpecProvisioner(first, AuditSpec(cfg), cfg).Apply(context.Background()); err != nil {
		t.Fatalf("primera corrida: %v", err)
	}
	second := &fakeS3{}
	if err := newSpecProvisioner(second, AuditSpec(cfg), cfg).Apply(context.Background()); err != nil {
		t.Fatalf("segunda corrida: %v", err)
	}

	for _, op := range []string{"PutPublicAccessBlock", "PutBucketEncryption", "PutBucketLifecycleConfiguration"} {
		if !reflect.DeepEqual(first.lastInput(t, op), second.lastInput(t, op)) {
			t.Errorf("%s: la segunda corrida aplicó una configuración distinta de la primera", op)
		}
	}
}
