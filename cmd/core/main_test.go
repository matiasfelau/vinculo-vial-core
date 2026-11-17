package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	amqplib "github.com/rabbitmq/amqp091-go"

	"github.com/vinculo-vial/core-service/internal/topology"
	"github.com/vinculo-vial/core-service/pkg/config"
)

const (
	testEvidenceBucket = "vinculo-vial-evidence-test"
	testAuditBucket    = "vinculo-vial-audit-test"
)

func discardLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// setEnv fija la configuración mínima válida del job, sin heredar nada del
// entorno de la máquina que corre las pruebas.
func setEnv(t *testing.T) {
	t.Helper()
	t.Setenv("RABBITMQ_URL", "amqp://usuario:clave@broker-de-prueba:5672/")
	t.Setenv("RABBITMQ_MAIN_EXCHANGE", "")
	t.Setenv("RABBITMQ_RETRY_EXCHANGE", "")
	t.Setenv("RABBITMQ_DLQ_EXCHANGE", "")
	t.Setenv("RABBITMQ_UNROUTED_EXCHANGE", "")
	t.Setenv("RETRY_TTL_MS", "")
	t.Setenv("ENV", "local")
	t.Setenv("S3_REGION", "us-east-1")
	t.Setenv("S3_TIMEOUT_MS", "")
	t.Setenv("EVIDENCE_BUCKET", testEvidenceBucket)
	t.Setenv("AUDIT_BUCKET", testAuditBucket)
	t.Setenv("EVIDENCE_PENDING_DAYS", "")
	t.Setenv("EVIDENCE_QUARANTINE_DAYS", "")
}

// fakeBroker simula un RabbitMQ: recuerda lo declarado y, como el real, la
// declaración pasiva falla si el elemento no existe. El estado sobrevive a
// varias corridas de run, lo que permite probar la idempotencia.
type fakeBroker struct {
	mu sync.Mutex

	exchanges map[string]bool
	queues    map[string]bool
	bindings  []string

	// failChannelAt hace fallar la apertura del canal número N (desde 1).
	failChannelAt  int
	channelsOpened int
	channelsClosed int
	closed         bool

	declareExchangeErr error
	declareQueueErr    error
	bindErr            error
	// hidePassive oculta los elementos a la declaración pasiva, como si la
	// declaración activa no hubiese tenido efecto en el broker.
	hidePassive bool
}

func newFakeBroker() *fakeBroker {
	return &fakeBroker{exchanges: map[string]bool{}, queues: map[string]bool{}}
}

func (b *fakeBroker) Channel() (channel, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.channelsOpened++
	if b.failChannelAt == b.channelsOpened {
		return nil, errors.New("no se pudo abrir el canal")
	}
	return &fakeChannel{b: b}, nil
}

func (b *fakeBroker) Close() error {
	b.closed = true
	return nil
}

type fakeChannel struct{ b *fakeBroker }

func (c *fakeChannel) ExchangeDeclare(name, _ string, _, _, _, _ bool, _ amqplib.Table) error {
	if c.b.declareExchangeErr != nil {
		return c.b.declareExchangeErr
	}
	c.b.exchanges[name] = true
	return nil
}

func (c *fakeChannel) QueueDeclare(name string, _, _, _, _ bool, _ amqplib.Table) (amqplib.Queue, error) {
	if c.b.declareQueueErr != nil {
		return amqplib.Queue{}, c.b.declareQueueErr
	}
	c.b.queues[name] = true
	return amqplib.Queue{Name: name}, nil
}

func (c *fakeChannel) QueueBind(name, key, exchange string, _ bool, _ amqplib.Table) error {
	if c.b.bindErr != nil {
		return c.b.bindErr
	}
	c.b.bindings = append(c.b.bindings, name+"|"+exchange+"|"+key)
	return nil
}

func (c *fakeChannel) ExchangeDeclarePassive(name, _ string, _, _, _, _ bool, _ amqplib.Table) error {
	if c.b.hidePassive || !c.b.exchanges[name] {
		return errors.New("NOT_FOUND - no exchange '" + name + "'")
	}
	return nil
}

func (c *fakeChannel) QueueDeclarePassive(name string, _, _, _, _ bool, _ amqplib.Table) (amqplib.Queue, error) {
	if c.b.hidePassive || !c.b.queues[name] {
		return amqplib.Queue{}, errors.New("NOT_FOUND - no queue '" + name + "'")
	}
	return amqplib.Queue{Name: name}, nil
}

func (c *fakeChannel) Close() error {
	c.b.channelsClosed++
	return nil
}

type fakeBucketState struct {
	pab       *types.PublicAccessBlockConfiguration
	enc       *types.ServerSideEncryptionConfiguration
	rules     []types.LifecycleRule
	objectLck bool
}

// fakeS3 simula S3 con estado: lo que Provisioner aplica es lo que
// BucketVerifier lee después, igual que contra el servicio real.
type fakeS3 struct {
	buckets map[string]*fakeBucketState
	// created registra los CreateBucket, en orden.
	created []string

	headErr error
	// ignorePAB descarta PutPublicAccessBlock: simula un bucket que quedó sin
	// Block Public Access.
	ignorePAB bool
	calls     int
}

func newFakeS3() *fakeS3 { return &fakeS3{buckets: map[string]*fakeBucketState{}} }

func notConfigured(code string) error { return &smithy.GenericAPIError{Code: code} }

func (f *fakeS3) HeadBucket(_ context.Context, in *s3.HeadBucketInput, _ ...func(*s3.Options)) (*s3.HeadBucketOutput, error) {
	f.calls++
	if f.headErr != nil {
		return nil, f.headErr
	}
	if _, ok := f.buckets[*in.Bucket]; !ok {
		return nil, &types.NotFound{}
	}
	return &s3.HeadBucketOutput{}, nil
}

func (f *fakeS3) CreateBucket(_ context.Context, in *s3.CreateBucketInput, _ ...func(*s3.Options)) (*s3.CreateBucketOutput, error) {
	f.calls++
	f.created = append(f.created, *in.Bucket)
	f.buckets[*in.Bucket] = &fakeBucketState{objectLck: in.ObjectLockEnabledForBucket != nil && *in.ObjectLockEnabledForBucket}
	return &s3.CreateBucketOutput{}, nil
}

func (f *fakeS3) PutPublicAccessBlock(_ context.Context, in *s3.PutPublicAccessBlockInput, _ ...func(*s3.Options)) (*s3.PutPublicAccessBlockOutput, error) {
	f.calls++
	if !f.ignorePAB {
		f.buckets[*in.Bucket].pab = in.PublicAccessBlockConfiguration
	}
	return &s3.PutPublicAccessBlockOutput{}, nil
}

func (f *fakeS3) PutBucketEncryption(_ context.Context, in *s3.PutBucketEncryptionInput, _ ...func(*s3.Options)) (*s3.PutBucketEncryptionOutput, error) {
	f.calls++
	f.buckets[*in.Bucket].enc = in.ServerSideEncryptionConfiguration
	return &s3.PutBucketEncryptionOutput{}, nil
}

func (f *fakeS3) PutBucketLifecycleConfiguration(_ context.Context, in *s3.PutBucketLifecycleConfigurationInput, _ ...func(*s3.Options)) (*s3.PutBucketLifecycleConfigurationOutput, error) {
	f.calls++
	f.buckets[*in.Bucket].rules = in.LifecycleConfiguration.Rules
	return &s3.PutBucketLifecycleConfigurationOutput{}, nil
}

func (f *fakeS3) GetPublicAccessBlock(_ context.Context, in *s3.GetPublicAccessBlockInput, _ ...func(*s3.Options)) (*s3.GetPublicAccessBlockOutput, error) {
	f.calls++
	b := f.buckets[*in.Bucket]
	if b.pab == nil {
		return nil, notConfigured("NoSuchPublicAccessBlockConfiguration")
	}
	return &s3.GetPublicAccessBlockOutput{PublicAccessBlockConfiguration: b.pab}, nil
}

func (f *fakeS3) GetBucketEncryption(_ context.Context, in *s3.GetBucketEncryptionInput, _ ...func(*s3.Options)) (*s3.GetBucketEncryptionOutput, error) {
	f.calls++
	b := f.buckets[*in.Bucket]
	if b.enc == nil {
		return nil, notConfigured("ServerSideEncryptionConfigurationNotFoundError")
	}
	return &s3.GetBucketEncryptionOutput{ServerSideEncryptionConfiguration: b.enc}, nil
}

func (f *fakeS3) GetBucketVersioning(_ context.Context, in *s3.GetBucketVersioningInput, _ ...func(*s3.Options)) (*s3.GetBucketVersioningOutput, error) {
	f.calls++
	out := &s3.GetBucketVersioningOutput{}
	if f.buckets[*in.Bucket].objectLck {
		// Object Lock activa el versionado al crear el bucket.
		out.Status = types.BucketVersioningStatusEnabled
	}
	return out, nil
}

func (f *fakeS3) GetBucketLifecycleConfiguration(_ context.Context, in *s3.GetBucketLifecycleConfigurationInput, _ ...func(*s3.Options)) (*s3.GetBucketLifecycleConfigurationOutput, error) {
	f.calls++
	b := f.buckets[*in.Bucket]
	if b.rules == nil {
		return nil, notConfigured("NoSuchLifecycleConfiguration")
	}
	return &s3.GetBucketLifecycleConfigurationOutput{Rules: b.rules}, nil
}

func (f *fakeS3) GetObjectLockConfiguration(_ context.Context, in *s3.GetObjectLockConfigurationInput, _ ...func(*s3.Options)) (*s3.GetObjectLockConfigurationOutput, error) {
	f.calls++
	if !f.buckets[*in.Bucket].objectLck {
		return nil, notConfigured("ObjectLockConfigurationNotFoundError")
	}
	return &s3.GetObjectLockConfigurationOutput{
		ObjectLockConfiguration: &types.ObjectLockConfiguration{ObjectLockEnabled: types.ObjectLockEnabledEnabled},
	}, nil
}

// testDeps arma las dependencias de run con los dobles dados. brokerErr y
// s3Err hacen fallar la conexión y la creación del cliente respectivamente.
func testDeps(b *fakeBroker, s *fakeS3, brokerErr, s3Err error) (deps, *int) {
	connects := 0
	return deps{
		connect: func(context.Context, config.Config, *slog.Logger) (broker, error) {
			connects++
			if brokerErr != nil {
				return nil, brokerErr
			}
			return b, nil
		},
		newS3: func(context.Context, config.StorageConfig) (s3API, error) {
			if s3Err != nil {
				return nil, s3Err
			}
			return s, nil
		},
	}, &connects
}

func TestRun_DeclaraYVerificaLaTopologiaYLosDosBuckets(t *testing.T) {
	setEnv(t)
	b, s := newFakeBroker(), newFakeS3()
	d, _ := testDeps(b, s, nil, nil)

	if err := run(context.Background(), discardLogger(), d); err != nil {
		t.Fatalf("error inesperado: %v", err)
	}

	cfg, err := config.Load()
	if err != nil {
		t.Fatal(err)
	}
	want := topology.Platform(cfg)
	if len(b.exchanges) != len(want.Exchanges) || len(b.queues) != len(want.Queues) || len(b.bindings) != len(want.Bindings) {
		t.Errorf("se declararon %d exchanges, %d colas y %d bindings; la topología de la plataforma tiene %d, %d y %d",
			len(b.exchanges), len(b.queues), len(b.bindings), len(want.Exchanges), len(want.Queues), len(want.Bindings))
	}
	for _, q := range want.Queues {
		if !b.queues[q.Name] {
			t.Errorf("la cola %q no se declaró", q.Name)
		}
	}
	if !slices.Equal(s.created, []string{testEvidenceBucket, testAuditBucket}) {
		t.Errorf("buckets creados = %v, se esperaba evidencia y luego Audit", s.created)
	}
	if !s.buckets[testAuditBucket].objectLck || s.buckets[testEvidenceBucket].objectLck {
		t.Error("solo el bucket de Audit se crea con Object Lock")
	}
	if !b.closed {
		t.Error("la conexión con el broker debe cerrarse al terminar")
	}
	if b.channelsOpened != 2 || b.channelsClosed != 2 {
		t.Errorf("canales abiertos/cerrados = %d/%d, se esperaban 2/2 (declaración y verificación)", b.channelsOpened, b.channelsClosed)
	}
}

func TestRun_EsIdempotenteSobreInfraestructuraYaDeclarada(t *testing.T) {
	setEnv(t)
	b, s := newFakeBroker(), newFakeS3()
	d, _ := testDeps(b, s, nil, nil)

	if err := run(context.Background(), discardLogger(), d); err != nil {
		t.Fatalf("primera corrida: %v", err)
	}
	if err := run(context.Background(), discardLogger(), d); err != nil {
		t.Fatalf("la segunda corrida sobre lo ya declarado debe terminar bien: %v", err)
	}
	if len(s.created) != 2 {
		t.Errorf("se crearon %d buckets en total: la segunda corrida no debe recrear los existentes", len(s.created))
	}
}

func TestRun_ConfiguracionInvalidaFallaAntesDeConectar(t *testing.T) {
	tests := []struct {
		name string
		env  string
		want string
	}{
		{"sin RABBITMQ_URL", "RABBITMQ_URL", "RABBITMQ_URL"},
		{"sin S3_REGION", "S3_REGION", "S3_REGION"},
		{"sin EVIDENCE_BUCKET", "EVIDENCE_BUCKET", "EVIDENCE_BUCKET"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			setEnv(t)
			t.Setenv(tt.env, "")
			d, connects := testDeps(newFakeBroker(), newFakeS3(), nil, nil)

			err := run(context.Background(), discardLogger(), d)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("error = %v, se esperaba mención a %s", err, tt.want)
			}
			if *connects != 0 {
				t.Error("con configuración inválida no se debe abrir ninguna conexión")
			}
		})
	}
}

func TestRun_ElBrokerNoDisponibleFallaYNoTocaS3(t *testing.T) {
	setEnv(t)
	cause := errors.New("connection refused")
	s := newFakeS3()
	d, _ := testDeps(newFakeBroker(), s, cause, nil)

	err := run(context.Background(), discardLogger(), d)
	if !errors.Is(err, cause) || !strings.Contains(err.Error(), "conectando con rabbitmq") {
		t.Fatalf("error = %v, se esperaba 'conectando con rabbitmq' envolviendo la causa", err)
	}
	if s.calls != 0 {
		t.Error("sin broker no se debe llamar a S3")
	}
}

func TestRun_FallasDeTopologiaCortanAntesDeS3(t *testing.T) {
	boom := errors.New("fallo del broker")
	tests := []struct {
		name  string
		setup func(*fakeBroker)
		want  string
	}{
		{"no abre el canal de declaración", func(b *fakeBroker) { b.failChannelAt = 1 }, "abriendo canal de declaración"},
		{"no abre el canal de verificación", func(b *fakeBroker) { b.failChannelAt = 2 }, "abriendo canal de verificación"},
		{"falla declarar un exchange", func(b *fakeBroker) { b.declareExchangeErr = boom }, "declarando topología"},
		{"falla declarar una cola", func(b *fakeBroker) { b.declareQueueErr = boom }, "declarando topología"},
		{"falla declarar un binding", func(b *fakeBroker) { b.bindErr = boom }, "declarando topología"},
		{"la verificación no encuentra lo declarado", func(b *fakeBroker) { b.hidePassive = true }, "verificando topología"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			setEnv(t)
			b, s := newFakeBroker(), newFakeS3()
			tt.setup(b)
			d, _ := testDeps(b, s, nil, nil)

			err := run(context.Background(), discardLogger(), d)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("error = %v, se esperaba %q", err, tt.want)
			}
			if s.calls != 0 {
				t.Error("si la topología falla no se debe seguir con los buckets")
			}
			if !b.closed {
				t.Error("la conexión debe cerrarse también cuando el job falla")
			}
			wantClosed := b.channelsOpened
			if b.failChannelAt > 0 {
				wantClosed-- // el canal que no llegó a abrirse no hay que cerrarlo
			}
			if b.channelsClosed != wantClosed {
				t.Errorf("canales abiertos = %d, cerrados = %d: todo canal abierto debe cerrarse", b.channelsOpened, b.channelsClosed)
			}
		})
	}
}

func TestRun_FallasDeS3SeReportanConElPasoQueFallo(t *testing.T) {
	tests := []struct {
		name  string
		setup func(*fakeS3)
		s3Err error
		want  string
	}{
		{"no crea el cliente", func(*fakeS3) {}, errors.New("sin credenciales"), "creando el cliente de S3"},
		{"el bucket queda sin Block Public Access", func(s *fakeS3) { s.ignorePAB = true }, nil, "verificando el bucket de evidencia"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			setEnv(t)
			s := newFakeS3()
			tt.setup(s)
			d, _ := testDeps(newFakeBroker(), s, nil, tt.s3Err)

			err := run(context.Background(), discardLogger(), d)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("error = %v, se esperaba %q", err, tt.want)
			}
		})
	}
}

func TestRun_ElAccesoDenegadoAlBucketDeEvidenciaEsFatalYNoTocaAudit(t *testing.T) {
	setEnv(t)
	s := newFakeS3()
	s.headErr = errForbidden()
	d, _ := testDeps(newFakeBroker(), s, nil, nil)

	err := run(context.Background(), discardLogger(), d)
	if err == nil || !strings.Contains(err.Error(), "declarando el bucket de evidencia") {
		t.Fatalf("error = %v, se esperaba 'declarando el bucket de evidencia'", err)
	}
	if len(s.created) != 0 {
		t.Errorf("buckets creados = %v: con acceso denegado no se crea nada", s.created)
	}
}

func TestRun_UnBucketDeAuditMalConfiguradoFallaEnLaVerificacion(t *testing.T) {
	setEnv(t)
	s := newFakeS3()
	// El bucket de Audit ya existe pero sin Object Lock: Core no puede
	// habilitarlo en un bucket existente y la verificación debe fallar.
	s.buckets[testAuditBucket] = &fakeBucketState{}
	d, _ := testDeps(newFakeBroker(), s, nil, nil)

	err := run(context.Background(), discardLogger(), d)
	if err == nil || !strings.Contains(err.Error(), "verificando el bucket de Audit") || !strings.Contains(err.Error(), "Object Lock") {
		t.Fatalf("error = %v, se esperaba falla de verificación del bucket de Audit por Object Lock", err)
	}
	if !slices.Contains(s.created, testEvidenceBucket) {
		t.Error("el bucket de evidencia se declara y verifica antes que el de Audit")
	}
}

func TestAmqpConnector_ElBrokerInaccesibleAgotaLaEsperaConLaCausa(t *testing.T) {
	cfg := config.Config{RabbitMQURL: "amqp://usuario:clave@127.0.0.1:1/"}
	connect := amqpConnector(40*time.Millisecond, 5*time.Millisecond)

	conn, err := connect(context.Background(), cfg, discardLogger())
	if err == nil {
		t.Fatalf("se esperaba error contra un puerto cerrado, se obtuvo la conexión %v", conn)
	}
	if conn != nil {
		t.Error("ante un error no debe devolverse una conexión")
	}
	if !strings.Contains(err.Error(), "no respondió") {
		t.Errorf("el error %q debería indicar que rabbitmq no respondió", err.Error())
	}
	if strings.Contains(err.Error(), "clave") {
		t.Errorf("el error %q no debe exponer las credenciales de la URL", err.Error())
	}
}

func TestDefaultDeps_ArmaLaConexionRealYElClienteDeS3(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("AWS_CONFIG_FILE", filepath.Join(dir, "config"))
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", filepath.Join(dir, "credentials"))
	t.Setenv("AWS_ACCESS_KEY_ID", "AKIATESTTESTTESTTEST")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "secreto-de-prueba")
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")

	d := defaultDeps()
	if d.connect == nil {
		t.Fatal("defaultDeps debe traer la conexión real con RabbitMQ")
	}

	client, err := d.newS3(context.Background(), config.StorageConfig{Region: "sa-east-1", Timeout: time.Second})
	if err != nil {
		t.Fatalf("error inesperado: %v", err)
	}
	real, ok := client.(*s3.Client)
	if !ok || real.Options().Region != "sa-east-1" {
		t.Errorf("el cliente de S3 debe ser un *s3.Client en la región configurada, se obtuvo %T", client)
	}
}

func errForbidden() error { return &forbiddenError{} }

type forbiddenError struct{}

func (*forbiddenError) Error() string       { return "access denied" }
func (*forbiddenError) HTTPStatusCode() int { return http.StatusForbidden }
