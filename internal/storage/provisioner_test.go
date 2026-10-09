package storage

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	smithyhttp "github.com/aws/smithy-go/transport/http"

	"github.com/vinculo-vial/core-service/pkg/config"
)

const testBucket = "vinculo-vial-evidence-test"

type call struct {
	op    string
	input any
}

// fakeS3 es un cliente S3 simulado: cada operación registra su llamada y
// devuelve el resultado programado en el campo de función correspondiente
// (nil significa éxito).
type fakeS3 struct {
	calls []call

	head      func(ctx context.Context, n int) error
	create    func() error
	putPAB    func() error
	putEnc    func() error
	putLife   func() error
	headCalls int
}

func (f *fakeS3) HeadBucket(ctx context.Context, in *s3.HeadBucketInput, _ ...func(*s3.Options)) (*s3.HeadBucketOutput, error) {
	f.headCalls++
	f.calls = append(f.calls, call{"HeadBucket", in})
	if f.head != nil {
		if err := f.head(ctx, f.headCalls); err != nil {
			return nil, err
		}
	}
	return &s3.HeadBucketOutput{}, nil
}

func (f *fakeS3) CreateBucket(_ context.Context, in *s3.CreateBucketInput, _ ...func(*s3.Options)) (*s3.CreateBucketOutput, error) {
	f.calls = append(f.calls, call{"CreateBucket", in})
	if f.create != nil {
		if err := f.create(); err != nil {
			return nil, err
		}
	}
	return &s3.CreateBucketOutput{}, nil
}

func (f *fakeS3) PutPublicAccessBlock(_ context.Context, in *s3.PutPublicAccessBlockInput, _ ...func(*s3.Options)) (*s3.PutPublicAccessBlockOutput, error) {
	f.calls = append(f.calls, call{"PutPublicAccessBlock", in})
	if f.putPAB != nil {
		if err := f.putPAB(); err != nil {
			return nil, err
		}
	}
	return &s3.PutPublicAccessBlockOutput{}, nil
}

func (f *fakeS3) PutBucketEncryption(_ context.Context, in *s3.PutBucketEncryptionInput, _ ...func(*s3.Options)) (*s3.PutBucketEncryptionOutput, error) {
	f.calls = append(f.calls, call{"PutBucketEncryption", in})
	if f.putEnc != nil {
		if err := f.putEnc(); err != nil {
			return nil, err
		}
	}
	return &s3.PutBucketEncryptionOutput{}, nil
}

func (f *fakeS3) PutBucketLifecycleConfiguration(_ context.Context, in *s3.PutBucketLifecycleConfigurationInput, _ ...func(*s3.Options)) (*s3.PutBucketLifecycleConfigurationOutput, error) {
	f.calls = append(f.calls, call{"PutBucketLifecycleConfiguration", in})
	if f.putLife != nil {
		if err := f.putLife(); err != nil {
			return nil, err
		}
	}
	return &s3.PutBucketLifecycleConfigurationOutput{}, nil
}

func (f *fakeS3) ops() []string {
	var ops []string
	for _, c := range f.calls {
		ops = append(ops, c.op)
	}
	return ops
}

func (f *fakeS3) countOf(op string) int {
	n := 0
	for _, c := range f.calls {
		if c.op == op {
			n++
		}
	}
	return n
}

func (f *fakeS3) lastInput(t *testing.T, op string) any {
	t.Helper()
	for i := len(f.calls) - 1; i >= 0; i-- {
		if f.calls[i].op == op {
			return f.calls[i].input
		}
	}
	t.Fatalf("no hubo ninguna llamada a %s", op)
	return nil
}

func testCfg() config.StorageConfig {
	return config.StorageConfig{
		Region:         "us-east-1",
		Bucket:         testBucket,
		Timeout:        200 * time.Millisecond,
		PendingDays:    2,
		QuarantineDays: 15,
	}
}

func testWait() WaitOptions {
	return WaitOptions{Timeout: 60 * time.Millisecond, InitialBackoff: time.Millisecond, MaxBackoff: 5 * time.Millisecond}
}

func newTestProvisioner(api API, cfg config.StorageConfig) *Provisioner {
	return NewProvisioner(api, cfg, testWait(), slog.New(slog.NewTextHandler(io.Discard, nil)))
}

func statusError(code int) error {
	return &awshttp.ResponseError{
		ResponseError: &smithyhttp.ResponseError{
			Response: &smithyhttp.Response{Response: &http.Response{StatusCode: code}},
			Err:      errors.New("respuesta de error"),
		},
	}
}

func connectionError() error {
	return &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connection refused")}
}

func TestApply_CreaElBucketSoloSiNoExiste(t *testing.T) {
	api := &fakeS3{head: func(context.Context, int) error { return &types.NotFound{} }}

	if err := newTestProvisioner(api, testCfg()).Apply(context.Background()); err != nil {
		t.Fatalf("error inesperado: %v", err)
	}

	want := []string{"HeadBucket", "CreateBucket", "PutPublicAccessBlock", "PutBucketEncryption", "PutBucketLifecycleConfiguration"}
	if got := api.ops(); !reflect.DeepEqual(got, want) {
		t.Fatalf("operaciones = %v, se esperaba %v", got, want)
	}

	create := api.lastInput(t, "CreateBucket").(*s3.CreateBucketInput)
	if aws.ToString(create.Bucket) != testBucket {
		t.Errorf("bucket creado = %q, se esperaba %q", aws.ToString(create.Bucket), testBucket)
	}
	if create.CreateBucketConfiguration != nil {
		t.Errorf("en us-east-1 no debe enviarse LocationConstraint, se envió %+v", create.CreateBucketConfiguration)
	}
}

func TestApply_NoCreaElBucketSiYaExiste(t *testing.T) {
	api := &fakeS3{}

	if err := newTestProvisioner(api, testCfg()).Apply(context.Background()); err != nil {
		t.Fatalf("error inesperado: %v", err)
	}

	if n := api.countOf("CreateBucket"); n != 0 {
		t.Fatalf("CreateBucket se llamó %d veces con el bucket existente", n)
	}
	for _, op := range []string{"PutPublicAccessBlock", "PutBucketEncryption", "PutBucketLifecycleConfiguration"} {
		if api.countOf(op) != 1 {
			t.Errorf("%s debe reaplicarse sobre un bucket existente", op)
		}
	}
}

func TestApply_FueraDeUSEast1EnviaLocationConstraint(t *testing.T) {
	cfg := testCfg()
	cfg.Region = "sa-east-1"
	api := &fakeS3{head: func(context.Context, int) error { return &types.NotFound{} }}

	if err := newTestProvisioner(api, cfg).Apply(context.Background()); err != nil {
		t.Fatalf("error inesperado: %v", err)
	}

	create := api.lastInput(t, "CreateBucket").(*s3.CreateBucketInput)
	if create.CreateBucketConfiguration == nil || create.CreateBucketConfiguration.LocationConstraint != "sa-east-1" {
		t.Fatalf("CreateBucketConfiguration = %+v, se esperaba LocationConstraint sa-east-1", create.CreateBucketConfiguration)
	}
}

func TestApply_BucketAlreadyOwnedByYouEsExito(t *testing.T) {
	api := &fakeS3{
		head:   func(context.Context, int) error { return &types.NotFound{} },
		create: func() error { return &types.BucketAlreadyOwnedByYou{} },
	}

	if err := newTestProvisioner(api, testCfg()).Apply(context.Background()); err != nil {
		t.Fatalf("BucketAlreadyOwnedByYou debe contar como éxito: %v", err)
	}
	if api.countOf("PutBucketLifecycleConfiguration") != 1 {
		t.Fatal("tras BucketAlreadyOwnedByYou se debe continuar con la configuración")
	}
}

func TestApply_BucketAlreadyExistsEsErrorAccionable(t *testing.T) {
	api := &fakeS3{
		head:   func(context.Context, int) error { return &types.NotFound{} },
		create: func() error { return &types.BucketAlreadyExists{} },
	}

	err := newTestProvisioner(api, testCfg()).Apply(context.Background())
	if err == nil {
		t.Fatal("se esperaba error: el nombre del bucket pertenece a otra cuenta")
	}
	for _, want := range []string{testBucket, "otra cuenta", "EVIDENCE_BUCKET"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("el error %q debería mencionar %q", err.Error(), want)
		}
	}
	if api.countOf("PutBucketLifecycleConfiguration") != 0 {
		t.Fatal("no se debe configurar un bucket que no es propio")
	}
}

func TestApply_LifecycleUsaLosDiasDelEntornoYSeAplicaCompleto(t *testing.T) {
	cfg := testCfg()
	cfg.PendingDays = 3
	cfg.QuarantineDays = 20
	api := &fakeS3{}

	if err := newTestProvisioner(api, cfg).Apply(context.Background()); err != nil {
		t.Fatalf("error inesperado: %v", err)
	}

	in := api.lastInput(t, "PutBucketLifecycleConfiguration").(*s3.PutBucketLifecycleConfigurationInput)
	rules := in.LifecycleConfiguration.Rules
	if len(rules) != 2 {
		t.Fatalf("el lifecycle tiene %d reglas, se esperaban exactamente 2 (reemplazo completo)", len(rules))
	}

	want := map[string]int32{StatusTagPending: 3, StatusTagQuarantine: 20}
	for _, r := range rules {
		tag := r.Filter.Tag
		days, ok := want[aws.ToString(tag.Value)]
		if !ok || aws.ToString(tag.Key) != StatusTagKey {
			t.Errorf("regla %q con tag inesperado %s=%s", aws.ToString(r.ID), aws.ToString(tag.Key), aws.ToString(tag.Value))
			continue
		}
		if aws.ToInt32(r.Expiration.Days) != days {
			t.Errorf("regla del tag %s expira a los %d días, se esperaban %d", aws.ToString(tag.Value), aws.ToInt32(r.Expiration.Days), days)
		}
		if r.Status != types.ExpirationStatusEnabled {
			t.Errorf("regla %q con estado %s, se esperaba Enabled", aws.ToString(r.ID), r.Status)
		}
		delete(want, aws.ToString(tag.Value))
	}
	if len(want) != 0 {
		t.Errorf("faltan reglas para los tags %v", want)
	}
}

func TestApply_AplicaAccesoPrivadoYCifradoSSES3(t *testing.T) {
	api := &fakeS3{}

	if err := newTestProvisioner(api, testCfg()).Apply(context.Background()); err != nil {
		t.Fatalf("error inesperado: %v", err)
	}

	pab := api.lastInput(t, "PutPublicAccessBlock").(*s3.PutPublicAccessBlockInput).PublicAccessBlockConfiguration
	for name, flag := range map[string]*bool{
		"BlockPublicAcls": pab.BlockPublicAcls, "BlockPublicPolicy": pab.BlockPublicPolicy,
		"IgnorePublicAcls": pab.IgnorePublicAcls, "RestrictPublicBuckets": pab.RestrictPublicBuckets,
	} {
		if !aws.ToBool(flag) {
			t.Errorf("%s debe estar en true", name)
		}
	}

	enc := api.lastInput(t, "PutBucketEncryption").(*s3.PutBucketEncryptionInput).ServerSideEncryptionConfiguration
	if len(enc.Rules) != 1 || enc.Rules[0].ApplyServerSideEncryptionByDefault.SSEAlgorithm != types.ServerSideEncryptionAes256 {
		t.Fatalf("cifrado = %+v, se esperaba una única regla AES256", enc)
	}
}

func TestApply_EsIdempotente(t *testing.T) {
	first := &fakeS3{head: func(context.Context, int) error { return &types.NotFound{} }}
	p := newTestProvisioner(first, testCfg())
	if err := p.Apply(context.Background()); err != nil {
		t.Fatalf("primera corrida: %v", err)
	}

	second := &fakeS3{}
	p = newTestProvisioner(second, testCfg())
	if err := p.Apply(context.Background()); err != nil {
		t.Fatalf("segunda corrida: %v", err)
	}

	if second.countOf("CreateBucket") != 0 {
		t.Error("la segunda corrida no debe crear el bucket")
	}
	for _, op := range []string{"PutPublicAccessBlock", "PutBucketEncryption", "PutBucketLifecycleConfiguration"} {
		if !reflect.DeepEqual(first.lastInput(t, op), second.lastInput(t, op)) {
			t.Errorf("%s: la segunda corrida aplicó una configuración distinta de la primera", op)
		}
	}
}

func TestApply_S3NoDisponibleFallaTrasLaEsperaAcotada(t *testing.T) {
	api := &fakeS3{head: func(context.Context, int) error { return connectionError() }}

	err := newTestProvisioner(api, testCfg()).Apply(context.Background())
	if err == nil {
		t.Fatal("se esperaba error con S3 no disponible")
	}
	if !strings.Contains(err.Error(), "no respondió") {
		t.Errorf("el error %q debería indicar que S3 no respondió", err.Error())
	}
	if api.headCalls < 2 {
		t.Errorf("HeadBucket se llamó %d veces, se esperaba al menos un reintento", api.headCalls)
	}
	if api.countOf("CreateBucket") != 0 || api.countOf("PutBucketLifecycleConfiguration") != 0 {
		t.Error("sin S3 disponible no se debe crear ni configurar nada")
	}
}

func TestApply_ReintentaUnFalloTransitorioYSeRecupera(t *testing.T) {
	api := &fakeS3{head: func(_ context.Context, n int) error {
		if n < 3 {
			return statusError(http.StatusServiceUnavailable)
		}
		return nil
	}}

	if err := newTestProvisioner(api, testCfg()).Apply(context.Background()); err != nil {
		t.Fatalf("error inesperado: %v", err)
	}
	if api.headCalls != 3 {
		t.Fatalf("HeadBucket se llamó %d veces, se esperaban 3", api.headCalls)
	}
}

func TestApply_TimeoutPorOperacionCuentaComoTransitorioYFallaAlAgotarLaEspera(t *testing.T) {
	cfg := testCfg()
	cfg.Timeout = 5 * time.Millisecond
	api := &fakeS3{head: func(ctx context.Context, _ int) error {
		<-ctx.Done()
		return ctx.Err()
	}}

	done := make(chan error, 1)
	go func() { done <- newTestProvisioner(api, cfg).Apply(context.Background()) }()

	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "no respondió") {
			t.Fatalf("error = %v, se esperaba que S3 no respondió", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("la espera no está acotada")
	}
}

func TestApply_AccesoDenegadoNoSeReintenta(t *testing.T) {
	api := &fakeS3{head: func(context.Context, int) error { return statusError(http.StatusForbidden) }}

	err := newTestProvisioner(api, testCfg()).Apply(context.Background())
	if err == nil || !strings.Contains(err.Error(), "acceso denegado") {
		t.Fatalf("error = %v, se esperaba acceso denegado", err)
	}
	if api.headCalls != 1 {
		t.Fatalf("HeadBucket se llamó %d veces, un 403 es permanente y no se reintenta", api.headCalls)
	}
}

func TestApply_BucketEnOtraRegionNoSeReintenta(t *testing.T) {
	api := &fakeS3{head: func(context.Context, int) error { return statusError(http.StatusMovedPermanently) }}

	err := newTestProvisioner(api, testCfg()).Apply(context.Background())
	if err == nil || !strings.Contains(err.Error(), "S3_REGION") {
		t.Fatalf("error = %v, se esperaba mención a S3_REGION", err)
	}
	if api.headCalls != 1 {
		t.Fatalf("HeadBucket se llamó %d veces, se esperaba 1", api.headCalls)
	}
}

func TestApply_ErrorDeConfiguracionAbortaYSeEnvuelveConContexto(t *testing.T) {
	boom := errors.New("fallo de la operación")
	api := &fakeS3{putEnc: func() error { return boom }}

	err := newTestProvisioner(api, testCfg()).Apply(context.Background())
	if !errors.Is(err, boom) {
		t.Fatalf("el error %v debería envolver la causa original", err)
	}
	if !strings.Contains(err.Error(), "cifrado") {
		t.Errorf("el error %q debería indicar qué paso falló", err.Error())
	}
	if api.countOf("PutBucketLifecycleConfiguration") != 0 {
		t.Error("tras un fallo no se debe seguir con los pasos siguientes")
	}
}

func TestApply_ContextoCanceladoDetieneLaEspera(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	api := &fakeS3{head: func(context.Context, int) error { return connectionError() }}

	err := newTestProvisioner(api, testCfg()).Apply(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, se esperaba context.Canceled", err)
	}
}
