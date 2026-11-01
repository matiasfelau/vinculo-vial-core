package verify

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"

	"github.com/vinculo-vial/core-service/internal/storage"
	"github.com/vinculo-vial/core-service/pkg/config"
)

// Códigos de error de S3 que indican que una configuración nunca fue
// aplicada al bucket (a diferencia de un fallo de la consulta en sí).
const (
	codeNoPublicAccessBlock = "NoSuchPublicAccessBlockConfiguration"
	codeNoEncryption        = "ServerSideEncryptionConfigurationNotFoundError"
	codeNoLifecycle         = "NoSuchLifecycleConfiguration"
)

// BucketAPI es el subconjunto del cliente S3 necesario para verificar el
// bucket. Se abstrae como interfaz para permitir testearlo con un mock, sin
// acceso a AWS.
type BucketAPI interface {
	HeadBucket(ctx context.Context, in *s3.HeadBucketInput, optFns ...func(*s3.Options)) (*s3.HeadBucketOutput, error)
	GetPublicAccessBlock(ctx context.Context, in *s3.GetPublicAccessBlockInput, optFns ...func(*s3.Options)) (*s3.GetPublicAccessBlockOutput, error)
	GetBucketEncryption(ctx context.Context, in *s3.GetBucketEncryptionInput, optFns ...func(*s3.Options)) (*s3.GetBucketEncryptionOutput, error)
	GetBucketVersioning(ctx context.Context, in *s3.GetBucketVersioningInput, optFns ...func(*s3.Options)) (*s3.GetBucketVersioningOutput, error)
	GetBucketLifecycleConfiguration(ctx context.Context, in *s3.GetBucketLifecycleConfigurationInput, optFns ...func(*s3.Options)) (*s3.GetBucketLifecycleConfigurationOutput, error)
}

// BucketVerifier confirma que el bucket de evidencia existe y que su
// configuración coincide con la que storage.Provisioner declara.
type BucketVerifier struct {
	api BucketAPI
	cfg config.StorageConfig
}

// NewBucketVerifier construye un BucketVerifier sobre el cliente dado.
func NewBucketVerifier(api BucketAPI, cfg config.StorageConfig) *BucketVerifier {
	return &BucketVerifier{api: api, cfg: cfg}
}

// Verify comprueba la existencia del bucket (HeadBucket) y, si existe, su
// Block Public Access, cifrado, versionado y lifecycle contra lo esperado.
// Cualquier diferencia o error de consulta falla (fail-closed); los
// problemas se reportan todos juntos para facilitar el diagnóstico.
func (v *BucketVerifier) Verify(ctx context.Context) error {
	if err := v.call(ctx, func(ctx context.Context) error {
		_, err := v.api.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(v.cfg.Bucket)})
		return err
	}); err != nil {
		return fmt.Errorf("el bucket %q no es accesible en S3: %w", v.cfg.Bucket, err)
	}

	problems := []error{
		v.checkPublicAccessBlock(ctx),
		v.checkEncryption(ctx),
		v.checkVersioning(ctx),
		v.checkLifecycle(ctx),
	}
	if err := errors.Join(problems...); err != nil {
		return fmt.Errorf("el bucket %q no coincide con lo declarado: %w", v.cfg.Bucket, err)
	}
	return nil
}

func (v *BucketVerifier) checkPublicAccessBlock(ctx context.Context) error {
	var out *s3.GetPublicAccessBlockOutput
	err := v.call(ctx, func(ctx context.Context) (err error) {
		out, err = v.api.GetPublicAccessBlock(ctx, &s3.GetPublicAccessBlockInput{Bucket: aws.String(v.cfg.Bucket)})
		return err
	})
	if hasErrorCode(err, codeNoPublicAccessBlock) {
		return errors.New("el Block Public Access no está configurado")
	}
	if err != nil {
		return fmt.Errorf("consultando Block Public Access: %w", err)
	}

	got := out.PublicAccessBlockConfiguration
	if got == nil {
		return errors.New("el Block Public Access no está configurado")
	}

	var off []string
	for name, flag := range map[string]*bool{
		"BlockPublicAcls":       got.BlockPublicAcls,
		"BlockPublicPolicy":     got.BlockPublicPolicy,
		"IgnorePublicAcls":      got.IgnorePublicAcls,
		"RestrictPublicBuckets": got.RestrictPublicBuckets,
	} {
		if !aws.ToBool(flag) {
			off = append(off, name)
		}
	}
	if len(off) > 0 {
		slices.Sort(off)
		return fmt.Errorf("el Block Public Access tiene indicadores desactivados: %s", strings.Join(off, ", "))
	}
	return nil
}

func (v *BucketVerifier) checkEncryption(ctx context.Context) error {
	var out *s3.GetBucketEncryptionOutput
	err := v.call(ctx, func(ctx context.Context) (err error) {
		out, err = v.api.GetBucketEncryption(ctx, &s3.GetBucketEncryptionInput{Bucket: aws.String(v.cfg.Bucket)})
		return err
	})
	if hasErrorCode(err, codeNoEncryption) {
		return errors.New("el cifrado por defecto no está configurado")
	}
	if err != nil {
		return fmt.Errorf("consultando el cifrado: %w", err)
	}

	if out.ServerSideEncryptionConfiguration == nil || len(out.ServerSideEncryptionConfiguration.Rules) != 1 {
		return errors.New("el cifrado por defecto debe tener exactamente una regla SSE-S3 (AES256)")
	}
	byDefault := out.ServerSideEncryptionConfiguration.Rules[0].ApplyServerSideEncryptionByDefault
	if byDefault == nil || byDefault.SSEAlgorithm != types.ServerSideEncryptionAes256 {
		return errors.New("el cifrado por defecto no es SSE-S3 (AES256)")
	}
	return nil
}

func (v *BucketVerifier) checkVersioning(ctx context.Context) error {
	var out *s3.GetBucketVersioningOutput
	err := v.call(ctx, func(ctx context.Context) (err error) {
		out, err = v.api.GetBucketVersioning(ctx, &s3.GetBucketVersioningInput{Bucket: aws.String(v.cfg.Bucket)})
		return err
	})
	if err != nil {
		return fmt.Errorf("consultando el versionado: %w", err)
	}

	if out.Status == types.BucketVersioningStatusEnabled {
		return errors.New("el versionado está habilitado y el contrato lo exige deshabilitado: " +
			"Core no lo modifica por su cuenta; suspenderlo o recrear el bucket vacío requiere una decisión manual")
	}
	return nil
}

func (v *BucketVerifier) checkLifecycle(ctx context.Context) error {
	var out *s3.GetBucketLifecycleConfigurationOutput
	err := v.call(ctx, func(ctx context.Context) (err error) {
		out, err = v.api.GetBucketLifecycleConfiguration(ctx, &s3.GetBucketLifecycleConfigurationInput{Bucket: aws.String(v.cfg.Bucket)})
		return err
	})
	if hasErrorCode(err, codeNoLifecycle) {
		return errors.New("el lifecycle no está configurado")
	}
	if err != nil {
		return fmt.Errorf("consultando el lifecycle: %w", err)
	}

	want := summarize(storage.LifecycleRules(v.cfg))
	got := summarize(out.Rules)
	if !slices.Equal(want, got) {
		return fmt.Errorf("el lifecycle difiere de lo declarado: se esperaba [%s], se encontró [%s]",
			strings.Join(want, "; "), strings.Join(got, "; "))
	}
	return nil
}

// call ejecuta una consulta con el timeout por operación de la
// configuración, sin exceder el deadline del contexto de entrada.
func (v *BucketVerifier) call(ctx context.Context, op func(context.Context) error) error {
	opCtx, cancel := context.WithTimeout(ctx, v.cfg.Timeout)
	defer cancel()
	return op(opCtx)
}

// summarize reduce cada regla a una descripción canónica y ordenada, de modo
// que comparar dos conjuntos sea independiente del orden y detecte también
// reglas extra o con acciones o filtros no declarados.
func summarize(rules []types.LifecycleRule) []string {
	out := make([]string, 0, len(rules))
	for _, r := range rules {
		out = append(out, summarizeRule(r))
	}
	slices.Sort(out)
	return out
}

func summarizeRule(r types.LifecycleRule) string {
	var tag, extraFilter string
	if f := r.Filter; f != nil {
		if f.Tag != nil {
			tag = aws.ToString(f.Tag.Key) + "=" + aws.ToString(f.Tag.Value)
		}
		if f.And != nil || aws.ToString(f.Prefix) != "" || f.ObjectSizeGreaterThan != nil || f.ObjectSizeLessThan != nil {
			extraFilter = " +filtro-adicional"
		}
	}

	var days int32
	if r.Expiration != nil {
		days = aws.ToInt32(r.Expiration.Days)
	}

	var extraActions string
	if len(r.Transitions) > 0 || r.NoncurrentVersionExpiration != nil || len(r.NoncurrentVersionTransitions) > 0 ||
		r.AbortIncompleteMultipartUpload != nil || (r.Expiration != nil && (r.Expiration.Date != nil || r.Expiration.ExpiredObjectDeleteMarker != nil)) {
		extraActions = " +acciones-adicionales"
	}

	return fmt.Sprintf("%s %s tag[%s] expira=%dd%s%s", aws.ToString(r.ID), r.Status, tag, days, extraFilter, extraActions)
}

// hasErrorCode indica si err es un error de API de S3 con el código dado.
func hasErrorCode(err error, code string) bool {
	var apiErr smithy.APIError
	return errors.As(err, &apiErr) && apiErr.ErrorCode() == code
}
