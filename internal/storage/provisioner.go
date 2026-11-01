package storage

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net"
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/vinculo-vial/core-service/pkg/config"
)

// API es el subconjunto del cliente S3 que el aprovisionador necesita. Se
// abstrae como interfaz para testearlo con un mock, sin acceso a AWS.
type API interface {
	HeadBucket(ctx context.Context, in *s3.HeadBucketInput, optFns ...func(*s3.Options)) (*s3.HeadBucketOutput, error)
	CreateBucket(ctx context.Context, in *s3.CreateBucketInput, optFns ...func(*s3.Options)) (*s3.CreateBucketOutput, error)
	PutPublicAccessBlock(ctx context.Context, in *s3.PutPublicAccessBlockInput, optFns ...func(*s3.Options)) (*s3.PutPublicAccessBlockOutput, error)
	PutBucketEncryption(ctx context.Context, in *s3.PutBucketEncryptionInput, optFns ...func(*s3.Options)) (*s3.PutBucketEncryptionOutput, error)
	PutBucketLifecycleConfiguration(ctx context.Context, in *s3.PutBucketLifecycleConfigurationInput, optFns ...func(*s3.Options)) (*s3.PutBucketLifecycleConfigurationOutput, error)
}

// WaitOptions acota la espera de disponibilidad de S3 al arrancar: el tope
// total y el backoff exponencial (con jitter) entre intentos.
type WaitOptions struct {
	Timeout        time.Duration
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
}

// permanentError marca un fallo que reintentar no puede resolver (permisos,
// región equivocada): la espera acotada lo devuelve de inmediato.
type permanentError struct{ err error }

func (e *permanentError) Error() string { return e.err.Error() }
func (e *permanentError) Unwrap() error { return e.err }

// Provisioner declara el bucket de evidencia en S3.
type Provisioner struct {
	api    API
	cfg    config.StorageConfig
	wait   WaitOptions
	logger *slog.Logger
}

// NewProvisioner construye un Provisioner sobre el cliente dado.
func NewProvisioner(api API, cfg config.StorageConfig, wait WaitOptions, logger *slog.Logger) *Provisioner {
	return &Provisioner{api: api, cfg: cfg, wait: wait, logger: logger}
}

// Apply espera a que S3 responda, crea el bucket solo si no existe y le
// reaplica la configuración completa: Block Public Access, cifrado SSE-S3 y
// lifecycle (reemplazo, no merge). Es idempotente: dos corridas seguidas
// terminan en éxito sin efectos. No toca el versionado: un bucket nuevo
// nace sin versionar y, si estuviera habilitado, lo detecta la verificación.
func (p *Provisioner) Apply(ctx context.Context) error {
	exists, err := p.awaitBucket(ctx)
	if err != nil {
		return err
	}

	if exists {
		p.logger.Info("bucket de evidencia existente, se reaplica su configuración", "bucket", p.cfg.Bucket)
	} else if err := p.createBucket(ctx); err != nil {
		return err
	}

	if err := p.putPublicAccessBlock(ctx); err != nil {
		return err
	}
	if err := p.putEncryption(ctx); err != nil {
		return err
	}
	return p.putLifecycle(ctx)
}

// awaitBucket sondea con HeadBucket hasta obtener una respuesta definitiva
// (existe o no existe) o agotar la espera. Solo reintenta fallos
// transitorios (red, timeout, 5xx), con backoff exponencial y jitter dentro
// del tope total; cualquier otro error se devuelve de inmediato.
func (p *Provisioner) awaitBucket(ctx context.Context) (bool, error) {
	deadline := time.Now().Add(p.wait.Timeout)
	backoff := p.wait.InitialBackoff

	for {
		exists, err := p.headBucket(ctx)
		if err == nil {
			return exists, nil
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			return false, fmt.Errorf("esperando a S3: %w", ctxErr)
		}
		if !isTransient(err) {
			return false, err
		}
		if !time.Now().Before(deadline) {
			return false, fmt.Errorf("s3 no respondió tras %s de espera: %w", p.wait.Timeout, err)
		}

		p.logger.Warn("s3 todavía no disponible, reintentando", "bucket", p.cfg.Bucket, "error", err)

		select {
		case <-ctx.Done():
			return false, fmt.Errorf("esperando a S3: %w", ctx.Err())
		case <-time.After(jitter(backoff)):
		}
		backoff = min(backoff*2, p.wait.MaxBackoff)
	}
}

func (p *Provisioner) headBucket(ctx context.Context) (bool, error) {
	err := p.call(ctx, func(ctx context.Context) error {
		_, err := p.api.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(p.cfg.Bucket)})
		return err
	})
	if err == nil {
		return true, nil
	}

	var notFound *types.NotFound
	if errors.As(err, &notFound) {
		return false, nil
	}

	switch statusCode(err) {
	case http.StatusNotFound:
		return false, nil
	case http.StatusForbidden:
		return false, &permanentError{fmt.Errorf("acceso denegado al bucket %q: existe en otra cuenta o a las credenciales les falta s3:ListBucket: %w", p.cfg.Bucket, err)}
	case http.StatusMovedPermanently:
		return false, &permanentError{fmt.Errorf("el bucket %q está en otra región distinta de S3_REGION=%q: %w", p.cfg.Bucket, p.cfg.Region, err)}
	}
	return false, fmt.Errorf("consultando el bucket %q: %w", p.cfg.Bucket, err)
}

func (p *Provisioner) createBucket(ctx context.Context) error {
	in := &s3.CreateBucketInput{Bucket: aws.String(p.cfg.Bucket)}
	if p.cfg.Region != regionUSEast1 {
		in.CreateBucketConfiguration = &types.CreateBucketConfiguration{
			LocationConstraint: types.BucketLocationConstraint(p.cfg.Region),
		}
	}

	err := p.call(ctx, func(ctx context.Context) error {
		_, err := p.api.CreateBucket(ctx, in)
		return err
	})

	var owned *types.BucketAlreadyOwnedByYou
	if errors.As(err, &owned) {
		p.logger.Info("el bucket de evidencia ya era propio", "bucket", p.cfg.Bucket)
		return nil
	}
	var taken *types.BucketAlreadyExists
	if errors.As(err, &taken) {
		return fmt.Errorf("el bucket %q ya existe y pertenece a otra cuenta: los nombres de bucket son globales, "+
			"definir otro EVIDENCE_BUCKET con un sufijo único: %w", p.cfg.Bucket, err)
	}
	if err != nil {
		return fmt.Errorf("creando el bucket %q: %w", p.cfg.Bucket, err)
	}

	p.logger.Info("bucket de evidencia creado", "bucket", p.cfg.Bucket, "region", p.cfg.Region)
	return nil
}

func (p *Provisioner) putPublicAccessBlock(ctx context.Context) error {
	err := p.call(ctx, func(ctx context.Context) error {
		_, err := p.api.PutPublicAccessBlock(ctx, &s3.PutPublicAccessBlockInput{
			Bucket:                         aws.String(p.cfg.Bucket),
			PublicAccessBlockConfiguration: PublicAccessBlockConfig(),
		})
		return err
	})
	if err != nil {
		return fmt.Errorf("aplicando Block Public Access al bucket %q: %w", p.cfg.Bucket, err)
	}
	p.logger.Info("block public access aplicado", "bucket", p.cfg.Bucket)
	return nil
}

func (p *Provisioner) putEncryption(ctx context.Context) error {
	err := p.call(ctx, func(ctx context.Context) error {
		_, err := p.api.PutBucketEncryption(ctx, &s3.PutBucketEncryptionInput{
			Bucket:                            aws.String(p.cfg.Bucket),
			ServerSideEncryptionConfiguration: EncryptionConfig(),
		})
		return err
	})
	if err != nil {
		return fmt.Errorf("aplicando el cifrado SSE-S3 al bucket %q: %w", p.cfg.Bucket, err)
	}
	p.logger.Info("cifrado por defecto aplicado", "bucket", p.cfg.Bucket)
	return nil
}

func (p *Provisioner) putLifecycle(ctx context.Context) error {
	err := p.call(ctx, func(ctx context.Context) error {
		_, err := p.api.PutBucketLifecycleConfiguration(ctx, &s3.PutBucketLifecycleConfigurationInput{
			Bucket:                 aws.String(p.cfg.Bucket),
			LifecycleConfiguration: &types.BucketLifecycleConfiguration{Rules: LifecycleRules(p.cfg)},
		})
		return err
	})
	if err != nil {
		return fmt.Errorf("aplicando el lifecycle al bucket %q: %w", p.cfg.Bucket, err)
	}
	p.logger.Info("lifecycle aplicado",
		"bucket", p.cfg.Bucket,
		"pending_days", p.cfg.PendingDays,
		"quarantine_days", p.cfg.QuarantineDays)
	return nil
}

// call ejecuta una operación con el timeout por operación de la
// configuración, sin exceder el deadline del contexto de entrada.
func (p *Provisioner) call(ctx context.Context, op func(context.Context) error) error {
	opCtx, cancel := context.WithTimeout(ctx, p.cfg.Timeout)
	defer cancel()
	return op(opCtx)
}

// isTransient indica si reintentar puede resolver el fallo: errores de red,
// timeouts y respuestas 5xx, 408 o 429. Todo lo demás (permisos,
// credenciales ausentes, región equivocada) es permanente.
func isTransient(err error) bool {
	var permanent *permanentError
	if errors.As(err, &permanent) {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}

	switch code := statusCode(err); {
	case code >= http.StatusInternalServerError:
		return true
	case code == http.StatusRequestTimeout, code == http.StatusTooManyRequests:
		return true
	}
	return false
}

// statusCode extrae el código HTTP de un error de respuesta del SDK, o 0 si
// el error no proviene de una respuesta HTTP.
func statusCode(err error) int {
	var withStatus interface{ HTTPStatusCode() int }
	if errors.As(err, &withStatus) {
		return withStatus.HTTPStatusCode()
	}
	return 0
}

// jitter devuelve una espera aleatoria en (0, d] (full jitter): evita que
// varios jobs arrancados a la vez reintenten sincronizados. No es un uso
// criptográfico, por lo que math/rand/v2 es suficiente.
func jitter(d time.Duration) time.Duration {
	if d <= 0 {
		return 0
	}
	return rand.N(d) + 1
}
