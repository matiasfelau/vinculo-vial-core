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
	codeNoObjectLock        = "ObjectLockConfigurationNotFoundError"
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
	GetObjectLockConfiguration(ctx context.Context, in *s3.GetObjectLockConfigurationInput, optFns ...func(*s3.Options)) (*s3.GetObjectLockConfigurationOutput, error)
}

// BucketVerifier confirma que un bucket existe y que su configuración
// coincide con la que storage.Provisioner declara para su BucketSpec.
type BucketVerifier struct {
	api  BucketAPI
	spec storage.BucketSpec
	cfg  config.StorageConfig
}

// NewBucketVerifier construye un BucketVerifier del bucket spec sobre el
// cliente dado. cfg aporta el timeout por operación.
func NewBucketVerifier(api BucketAPI, spec storage.BucketSpec, cfg config.StorageConfig) *BucketVerifier {
	return &BucketVerifier{api: api, spec: spec, cfg: cfg}
}

// Verify comprueba la existencia del bucket (HeadBucket) y, si existe, su
// Block Public Access, cifrado, versionado, lifecycle y, cuando la
// especificación lo pide, Object Lock contra lo esperado. Cualquier
// diferencia o error de consulta falla (fail-closed); los problemas se
// reportan todos juntos para facilitar el diagnóstico.
func (v *BucketVerifier) Verify(ctx context.Context) error {
	if err := v.call(ctx, func(ctx context.Context) error {
		_, err := v.api.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(v.spec.Name)})
		return err
	}); err != nil {
		return fmt.Errorf("el bucket %q no es accesible en S3: %w", v.spec.Name, err)
	}

	problems := []error{
		v.checkPublicAccessBlock(ctx),
		v.checkEncryption(ctx),
		v.checkVersioning(ctx),
		v.checkLifecycle(ctx),
	}
	if v.spec.ObjectLock {
		problems = append(problems, v.checkObjectLock(ctx))
	}
	if err := errors.Join(problems...); err != nil {
		return fmt.Errorf("el bucket %q no coincide con lo declarado: %w", v.spec.Name, err)
	}
	return nil
}

func (v *BucketVerifier) checkPublicAccessBlock(ctx context.Context) error {
	var out *s3.GetPublicAccessBlockOutput
	err := v.call(ctx, func(ctx context.Context) (err error) {
		out, err = v.api.GetPublicAccessBlock(ctx, &s3.GetPublicAccessBlockInput{Bucket: aws.String(v.spec.Name)})
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
		out, err = v.api.GetBucketEncryption(ctx, &s3.GetBucketEncryptionInput{Bucket: aws.String(v.spec.Name)})
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
		out, err = v.api.GetBucketVersioning(ctx, &s3.GetBucketVersioningInput{Bucket: aws.String(v.spec.Name)})
		return err
	})
	if err != nil {
		return fmt.Errorf("consultando el versionado: %w", err)
	}

	enabled := out.Status == types.BucketVersioningStatusEnabled
	switch {
	case v.spec.ObjectLock && !enabled:
		return fmt.Errorf("el versionado no está habilitado y el bucket con Object Lock lo exige (estado %q): "+
			"Core no lo modifica por su cuenta", out.Status)
	case !v.spec.ObjectLock && enabled:
		return errors.New("el versionado está habilitado y el contrato lo exige deshabilitado: " +
			"Core no lo modifica por su cuenta; suspenderlo o recrear el bucket vacío requiere una decisión manual")
	}
	return nil
}

// checkObjectLock exige Object Lock habilitado y sin retención por defecto:
// la retención la fija el dueño del bucket en cada objeto. Object Lock solo
// se puede habilitar al crear el bucket, así que ante un bucket existente que
// no lo tiene la única salida es recrearlo, decisión que Core no toma.
func (v *BucketVerifier) checkObjectLock(ctx context.Context) error {
	var out *s3.GetObjectLockConfigurationOutput
	err := v.call(ctx, func(ctx context.Context) (err error) {
		out, err = v.api.GetObjectLockConfiguration(ctx, &s3.GetObjectLockConfigurationInput{Bucket: aws.String(v.spec.Name)})
		return err
	})
	if hasErrorCode(err, codeNoObjectLock) {
		return v.objectLockDisabled()
	}
	if err != nil {
		return fmt.Errorf("consultando Object Lock: %w", err)
	}

	cfg := out.ObjectLockConfiguration
	if cfg == nil || cfg.ObjectLockEnabled != types.ObjectLockEnabledEnabled {
		return v.objectLockDisabled()
	}
	if cfg.Rule != nil {
		return errors.New("Object Lock tiene una retención por defecto y el contrato no la admite: " +
			"la fija el dueño del bucket en cada objeto")
	}
	return nil
}

func (v *BucketVerifier) objectLockDisabled() error {
	return fmt.Errorf("el bucket existe sin Object Lock y no se puede habilitar en un bucket existente: "+
		"Core no lo borra ni lo recrea; hay que eliminarlo a mano (vacío) o definir otro %s", v.spec.EnvVar)
}

func (v *BucketVerifier) checkLifecycle(ctx context.Context) error {
	var out *s3.GetBucketLifecycleConfigurationOutput
	err := v.call(ctx, func(ctx context.Context) (err error) {
		out, err = v.api.GetBucketLifecycleConfiguration(ctx, &s3.GetBucketLifecycleConfigurationInput{Bucket: aws.String(v.spec.Name)})
		return err
	})
	if hasErrorCode(err, codeNoLifecycle) {
		return errors.New("el lifecycle no está configurado")
	}
	if err != nil {
		return fmt.Errorf("consultando el lifecycle: %w", err)
	}

	want := summarize(v.spec.Rules)
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

	// La expiración por días solo se resume si la regla la tiene: la regla de
	// Audit no expira objetos vigentes.
	var expira string
	if r.Expiration != nil && r.Expiration.Days != nil {
		expira = fmt.Sprintf(" expira=%dd", aws.ToInt32(r.Expiration.Days))
	}

	// Las acciones de versiones no vigentes y de delete markers se resumen con
	// sus valores (las usa el lifecycle de Audit); cualquier otra acción no
	// declarada se marca como adicional.
	var noncurrent, deleteMarker string
	if r.NoncurrentVersionExpiration != nil {
		noncurrent = fmt.Sprintf(" noncurrent=%dd", aws.ToInt32(r.NoncurrentVersionExpiration.NoncurrentDays))
	}
	if r.Expiration != nil && aws.ToBool(r.Expiration.ExpiredObjectDeleteMarker) {
		deleteMarker = " delete-marker=true"
	}

	var extraActions string
	if len(r.Transitions) > 0 || len(r.NoncurrentVersionTransitions) > 0 || r.AbortIncompleteMultipartUpload != nil ||
		(r.NoncurrentVersionExpiration != nil && r.NoncurrentVersionExpiration.NewerNoncurrentVersions != nil) ||
		(r.Expiration != nil && r.Expiration.Date != nil) {
		extraActions = " +acciones-adicionales"
	}

	return fmt.Sprintf("%s %s tag[%s]%s%s%s%s%s", aws.ToString(r.ID), r.Status, tag, expira, noncurrent, deleteMarker, extraFilter, extraActions)
}

// hasErrorCode indica si err es un error de API de S3 con el código dado.
func hasErrorCode(err error, code string) bool {
	var apiErr smithy.APIError
	return errors.As(err, &apiErr) && apiErr.ErrorCode() == code
}
