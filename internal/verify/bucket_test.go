package verify

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"

	"github.com/vinculo-vial/core-service/internal/storage"
	"github.com/vinculo-vial/core-service/pkg/config"
)

func bucketTestConfig() config.StorageConfig {
	return config.StorageConfig{
		Region:         "us-east-1",
		Bucket:         "vinculo-vial-evidence-test",
		Timeout:        time.Second,
		PendingDays:    2,
		QuarantineDays: 15,
	}
}

// fakeBucket es un cliente S3 simulado que por defecto devuelve la
// configuración correcta; cada test pisa solo el campo que quiere romper.
type fakeBucket struct {
	head       error
	pab        *s3.GetPublicAccessBlockOutput
	pabErr     error
	enc        *s3.GetBucketEncryptionOutput
	encErr     error
	versioning *s3.GetBucketVersioningOutput
	versErr    error
	lifecycle  *s3.GetBucketLifecycleConfigurationOutput
	lifeErr    error

	calls []string
}

func validBucket(cfg config.StorageConfig) *fakeBucket {
	return &fakeBucket{
		pab:        &s3.GetPublicAccessBlockOutput{PublicAccessBlockConfiguration: storage.PublicAccessBlockConfig()},
		enc:        &s3.GetBucketEncryptionOutput{ServerSideEncryptionConfiguration: storage.EncryptionConfig()},
		versioning: &s3.GetBucketVersioningOutput{},
		lifecycle:  &s3.GetBucketLifecycleConfigurationOutput{Rules: storage.LifecycleRules(cfg)},
	}
}

func (f *fakeBucket) HeadBucket(context.Context, *s3.HeadBucketInput, ...func(*s3.Options)) (*s3.HeadBucketOutput, error) {
	f.calls = append(f.calls, "HeadBucket")
	if f.head != nil {
		return nil, f.head
	}
	return &s3.HeadBucketOutput{}, nil
}

func (f *fakeBucket) GetPublicAccessBlock(context.Context, *s3.GetPublicAccessBlockInput, ...func(*s3.Options)) (*s3.GetPublicAccessBlockOutput, error) {
	f.calls = append(f.calls, "GetPublicAccessBlock")
	return f.pab, f.pabErr
}

func (f *fakeBucket) GetBucketEncryption(context.Context, *s3.GetBucketEncryptionInput, ...func(*s3.Options)) (*s3.GetBucketEncryptionOutput, error) {
	f.calls = append(f.calls, "GetBucketEncryption")
	return f.enc, f.encErr
}

func (f *fakeBucket) GetBucketVersioning(context.Context, *s3.GetBucketVersioningInput, ...func(*s3.Options)) (*s3.GetBucketVersioningOutput, error) {
	f.calls = append(f.calls, "GetBucketVersioning")
	return f.versioning, f.versErr
}

func (f *fakeBucket) GetBucketLifecycleConfiguration(context.Context, *s3.GetBucketLifecycleConfigurationInput, ...func(*s3.Options)) (*s3.GetBucketLifecycleConfigurationOutput, error) {
	f.calls = append(f.calls, "GetBucketLifecycleConfiguration")
	return f.lifecycle, f.lifeErr
}

func apiError(code string) error {
	return &smithy.GenericAPIError{Code: code, Message: "no configurado"}
}

func mustFailWith(t *testing.T, f *fakeBucket, cfg config.StorageConfig, wants ...string) {
	t.Helper()

	err := NewBucketVerifier(f, cfg).Verify(context.Background())
	if err == nil {
		t.Fatalf("se esperaba un error que mencione %q", wants)
	}
	for _, want := range wants {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("el error %q debería mencionar %q", err.Error(), want)
		}
	}
}

func TestBucketVerify_ConfiguracionCorrecta(t *testing.T) {
	cfg := bucketTestConfig()

	if err := NewBucketVerifier(validBucket(cfg), cfg).Verify(context.Background()); err != nil {
		t.Fatalf("error inesperado: %v", err)
	}
}

func TestBucketVerify_AceptaVersionadoNuncaActivadoOSuspendido(t *testing.T) {
	cfg := bucketTestConfig()

	for _, status := range []types.BucketVersioningStatus{"", types.BucketVersioningStatusSuspended} {
		f := validBucket(cfg)
		f.versioning = &s3.GetBucketVersioningOutput{Status: status}

		if err := NewBucketVerifier(f, cfg).Verify(context.Background()); err != nil {
			t.Errorf("versionado %q: error inesperado: %v", status, err)
		}
	}
}

func TestBucketVerify_VersionadoHabilitadoEsError(t *testing.T) {
	cfg := bucketTestConfig()
	f := validBucket(cfg)
	f.versioning = &s3.GetBucketVersioningOutput{Status: types.BucketVersioningStatusEnabled}

	mustFailWith(t, f, cfg, "versionado está habilitado", "decisión manual")
}

func TestBucketVerify_BucketInexistenteFallaSinConsultarLaConfiguracion(t *testing.T) {
	cfg := bucketTestConfig()
	f := validBucket(cfg)
	f.head = &types.NotFound{}

	mustFailWith(t, f, cfg, "no es accesible")
	if len(f.calls) != 1 {
		t.Fatalf("tras un HeadBucket fallido no se debe seguir consultando, llamadas: %v", f.calls)
	}
}

func TestBucketVerify_LifecycleFaltanteEsError(t *testing.T) {
	cfg := bucketTestConfig()
	f := validBucket(cfg)
	f.lifecycle = nil
	f.lifeErr = apiError("NoSuchLifecycleConfiguration")

	mustFailWith(t, f, cfg, "lifecycle no está configurado")
}

func TestBucketVerify_LifecycleConDiasDistintosDeLosDelEntorno(t *testing.T) {
	cfg := bucketTestConfig()
	cfg.PendingDays = 3
	cfg.QuarantineDays = 20

	stale := bucketTestConfig() // el bucket quedó con los días anteriores (2 y 15)
	f := validBucket(stale)

	mustFailWith(t, f, cfg, "lifecycle difiere", "expira=3d", "expira=2d")
}

func TestBucketVerify_LifecycleUsaLosDiasDelEntorno(t *testing.T) {
	cfg := bucketTestConfig()
	cfg.PendingDays = 3
	cfg.QuarantineDays = 20

	if err := NewBucketVerifier(validBucket(cfg), cfg).Verify(context.Background()); err != nil {
		t.Fatalf("error inesperado con los días del entorno: %v", err)
	}
}

func TestBucketVerify_LifecycleConReglaFaltante(t *testing.T) {
	cfg := bucketTestConfig()
	f := validBucket(cfg)
	f.lifecycle = &s3.GetBucketLifecycleConfigurationOutput{Rules: storage.LifecycleRules(cfg)[:1]}

	mustFailWith(t, f, cfg, "lifecycle difiere", storage.RuleQuarantineID)
}

func TestBucketVerify_LifecycleConReglaExtra(t *testing.T) {
	cfg := bucketTestConfig()
	f := validBucket(cfg)
	extra := types.LifecycleRule{
		ID:         aws.String("regla-ajena"),
		Status:     types.ExpirationStatusEnabled,
		Filter:     &types.LifecycleRuleFilter{Prefix: aws.String("alerts/")},
		Expiration: &types.LifecycleExpiration{Days: aws.Int32(1)},
	}
	f.lifecycle = &s3.GetBucketLifecycleConfigurationOutput{Rules: append(storage.LifecycleRules(cfg), extra)}

	mustFailWith(t, f, cfg, "lifecycle difiere", "regla-ajena")
}

func TestBucketVerify_LifecycleConReglaDeshabilitada(t *testing.T) {
	cfg := bucketTestConfig()
	f := validBucket(cfg)
	rules := storage.LifecycleRules(cfg)
	rules[0].Status = types.ExpirationStatusDisabled
	f.lifecycle = &s3.GetBucketLifecycleConfigurationOutput{Rules: rules}

	mustFailWith(t, f, cfg, "lifecycle difiere", string(types.ExpirationStatusDisabled))
}

func TestBucketVerify_LifecycleConTagDistinto(t *testing.T) {
	cfg := bucketTestConfig()
	f := validBucket(cfg)
	rules := storage.LifecycleRules(cfg)
	rules[0].Filter.Tag.Value = aws.String("otro")
	f.lifecycle = &s3.GetBucketLifecycleConfigurationOutput{Rules: rules}

	mustFailWith(t, f, cfg, "lifecycle difiere", "status=otro")
}

func TestBucketVerify_LifecycleNoDependeDelOrdenDeLasReglas(t *testing.T) {
	cfg := bucketTestConfig()
	f := validBucket(cfg)
	rules := storage.LifecycleRules(cfg)
	f.lifecycle = &s3.GetBucketLifecycleConfigurationOutput{Rules: []types.LifecycleRule{rules[1], rules[0]}}

	if err := NewBucketVerifier(f, cfg).Verify(context.Background()); err != nil {
		t.Fatalf("el orden de las reglas no debe importar: %v", err)
	}
}

func TestBucketVerify_PublicAccessBlockFaltanteEsError(t *testing.T) {
	cfg := bucketTestConfig()
	f := validBucket(cfg)
	f.pab = nil
	f.pabErr = apiError("NoSuchPublicAccessBlockConfiguration")

	mustFailWith(t, f, cfg, "Block Public Access no está configurado")
}

func TestBucketVerify_PublicAccessBlockConIndicadoresDesactivados(t *testing.T) {
	cfg := bucketTestConfig()
	f := validBucket(cfg)
	f.pab = &s3.GetPublicAccessBlockOutput{PublicAccessBlockConfiguration: &types.PublicAccessBlockConfiguration{
		BlockPublicAcls:       aws.Bool(true),
		BlockPublicPolicy:     aws.Bool(false),
		IgnorePublicAcls:      aws.Bool(true),
		RestrictPublicBuckets: nil,
	}}

	mustFailWith(t, f, cfg, "BlockPublicPolicy", "RestrictPublicBuckets")
}

func TestBucketVerify_CifradoFaltanteEsError(t *testing.T) {
	cfg := bucketTestConfig()
	f := validBucket(cfg)
	f.enc = nil
	f.encErr = apiError("ServerSideEncryptionConfigurationNotFoundError")

	mustFailWith(t, f, cfg, "cifrado por defecto no está configurado")
}

func TestBucketVerify_CifradoDistintoDeSSES3(t *testing.T) {
	cfg := bucketTestConfig()
	f := validBucket(cfg)
	f.enc = &s3.GetBucketEncryptionOutput{ServerSideEncryptionConfiguration: &types.ServerSideEncryptionConfiguration{
		Rules: []types.ServerSideEncryptionRule{{
			ApplyServerSideEncryptionByDefault: &types.ServerSideEncryptionByDefault{SSEAlgorithm: types.ServerSideEncryptionAwsKms},
		}},
	}}

	mustFailWith(t, f, cfg, "no es SSE-S3")
}

func TestBucketVerify_ErrorDeConsultaEsFailClosed(t *testing.T) {
	cfg := bucketTestConfig()
	boom := errors.New("fallo de red")

	cases := map[string]func(*fakeBucket){
		"public access block": func(f *fakeBucket) { f.pab, f.pabErr = nil, boom },
		"cifrado":             func(f *fakeBucket) { f.enc, f.encErr = nil, boom },
		"versionado":          func(f *fakeBucket) { f.versioning, f.versErr = nil, boom },
		"lifecycle":           func(f *fakeBucket) { f.lifecycle, f.lifeErr = nil, boom },
	}
	for name, breakIt := range cases {
		t.Run(name, func(t *testing.T) {
			f := validBucket(cfg)
			breakIt(f)

			err := NewBucketVerifier(f, cfg).Verify(context.Background())
			if !errors.Is(err, boom) {
				t.Fatalf("error = %v, se esperaba que envolviera la falla de consulta", err)
			}
		})
	}
}

func TestBucketVerify_ReportaTodosLosProblemasJuntos(t *testing.T) {
	cfg := bucketTestConfig()
	f := validBucket(cfg)
	f.pab, f.pabErr = nil, apiError("NoSuchPublicAccessBlockConfiguration")
	f.lifecycle, f.lifeErr = nil, apiError("NoSuchLifecycleConfiguration")
	f.versioning = &s3.GetBucketVersioningOutput{Status: types.BucketVersioningStatusEnabled}

	mustFailWith(t, f, cfg, "Block Public Access", "lifecycle", "versionado")
}
