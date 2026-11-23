package verify

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/vinculo-vial/core-service/internal/storage"
)

func TestBucketVerify_PublicAccessBlockSinConfiguracionEnLaRespuestaEsError(t *testing.T) {
	cfg := bucketTestConfig()
	f := validBucket(cfg)
	f.pab = &s3.GetPublicAccessBlockOutput{}

	mustFailWith(t, f, cfg, "Block Public Access no está configurado")
}

func TestBucketVerify_CifradoConCantidadDeReglasDistintaDeUnaEsError(t *testing.T) {
	cfg := bucketTestConfig()
	sse := types.ServerSideEncryptionRule{
		ApplyServerSideEncryptionByDefault: &types.ServerSideEncryptionByDefault{SSEAlgorithm: types.ServerSideEncryptionAes256},
	}

	cases := map[string]*s3.GetBucketEncryptionOutput{
		"sin configuración": {},
		"sin reglas":        {ServerSideEncryptionConfiguration: &types.ServerSideEncryptionConfiguration{}},
		"dos reglas":        {ServerSideEncryptionConfiguration: &types.ServerSideEncryptionConfiguration{Rules: []types.ServerSideEncryptionRule{sse, sse}}},
	}
	for name, enc := range cases {
		t.Run(name, func(t *testing.T) {
			f := validBucket(cfg)
			f.enc = enc

			mustFailWith(t, f, cfg, "exactamente una regla SSE-S3")
		})
	}
}

func TestBucketVerify_CifradoSinCifradoPorDefectoEnLaReglaEsError(t *testing.T) {
	cfg := bucketTestConfig()
	f := validBucket(cfg)
	f.enc = &s3.GetBucketEncryptionOutput{ServerSideEncryptionConfiguration: &types.ServerSideEncryptionConfiguration{
		Rules: []types.ServerSideEncryptionRule{{}},
	}}

	mustFailWith(t, f, cfg, "no es SSE-S3")
}

func TestBucketVerify_LifecycleConAccionesNoDeclaradasSeMarcaComoAdicional(t *testing.T) {
	cfg := bucketTestConfig()

	cases := map[string]func(*types.LifecycleRule){
		"transición": func(r *types.LifecycleRule) {
			r.Transitions = []types.Transition{{Days: aws.Int32(30), StorageClass: types.TransitionStorageClassGlacier}}
		},
		"transición de versiones no vigentes": func(r *types.LifecycleRule) {
			r.NoncurrentVersionTransitions = []types.NoncurrentVersionTransition{{NoncurrentDays: aws.Int32(30), StorageClass: types.TransitionStorageClassGlacier}}
		},
		"aborto de multipart": func(r *types.LifecycleRule) {
			r.AbortIncompleteMultipartUpload = &types.AbortIncompleteMultipartUpload{DaysAfterInitiation: aws.Int32(1)}
		},
		"versiones no vigentes retenidas": func(r *types.LifecycleRule) {
			r.NoncurrentVersionExpiration = &types.NoncurrentVersionExpiration{NoncurrentDays: aws.Int32(1), NewerNoncurrentVersions: aws.Int32(3)}
		},
		"expiración por fecha": func(r *types.LifecycleRule) {
			r.Expiration = &types.LifecycleExpiration{Date: aws.Time(testDate)}
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			f := validBucket(cfg)
			rules := storage.LifecycleRules(cfg)
			mutate(&rules[0])
			f.lifecycle = &s3.GetBucketLifecycleConfigurationOutput{Rules: rules}

			mustFailWith(t, f, cfg, "lifecycle difiere", "+acciones-adicionales")
		})
	}
}

var testDate = time.Date(2030, time.January, 1, 0, 0, 0, 0, time.UTC)
