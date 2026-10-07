package storage

import (
	"context"
	"fmt"

	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/vinculo-vial/core-service/pkg/config"
)

// noSDKRetries desactiva los reintentos internos del SDK: la única espera
// admitida es la acotada y con backoff de Provisioner, que cuenta cada
// intento y respeta el deadline total.
const noSDKRetries = 1

// NewClient construye el cliente S3 con la cadena de credenciales estándar
// del SDK (variables AWS_* en local, rol IAM en producción), con la región
// configurada y un timeout de cfg.Timeout en el cliente HTTP.
func NewClient(ctx context.Context, cfg config.StorageConfig) (*s3.Client, error) {
	httpClient := awshttp.NewBuildableClient().WithTimeout(cfg.Timeout)

	awsCfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion(cfg.Region),
		awsconfig.WithHTTPClient(httpClient),
		awsconfig.WithRetryMaxAttempts(noSDKRetries),
	)
	if err != nil {
		return nil, fmt.Errorf("cargando la configuración del SDK de AWS: %w", err)
	}

	return s3.NewFromConfig(awsCfg), nil
}
