# vinculo-vial-core

Job de inicialización de infraestructura del ecosistema Vínculo Vial: declara,
de forma idempotente, la topología de RabbitMQ (exchanges, colas, bindings,
reintento y dead-letter) y el bucket de evidencia en AWS S3, los verifica y
termina. No consume ni publica eventos de negocio, ni lee ni escribe objetos.

## Requisitos

- Go 1.25+
- Docker y Docker Compose

## Comandos

```
go run ./cmd/core
go test ./...
go test -tags aws ./internal/storage/...      # opt-in, bucket de desarrollo real
CGO_ENABLED=0 go build -o core ./cmd/core
docker compose up --build
```

`go run ./cmd/core` y `docker compose up` requieren la configuración del
bucket (ver la sección siguiente): sin `S3_REGION`, `EVIDENCE_BUCKET` y
credenciales AWS, el job termina con error.

## Bucket de evidencia (AWS S3)

Core es el único que declara el bucket de evidencia. En cada corrida:

1. Espera a que S3 responda (espera acotada con backoff exponencial y jitter;
   un fallo permanente, como un 403, no se reintenta).
2. Crea el bucket solo si no existe, con `LocationConstraint` salvo en
   `us-east-1`. `BucketAlreadyOwnedByYou` cuenta como éxito;
   `BucketAlreadyExists` es un error accionable: el nombre es de otra cuenta.
3. Reaplica la configuración completa: Block Public Access con los cuatro
   indicadores en `true`, cifrado SSE-S3 (`AES256`) y el lifecycle.
4. Verifica con `HeadBucket` y los `Get` de Block Public Access, cifrado,
   versionado y lifecycle. Cualquier diferencia o error termina con código
   distinto de 0.

El lifecycle se aplica completo en cada corrida (reemplazo, no merge): los
objetos con el tag `status=pending` expiran a los `EVIDENCE_PENDING_DAYS` días
y los de `status=quarantine`, a los `EVIDENCE_QUARANTINE_DAYS`. Una regla
agregada a mano al bucket se pisa en la próxima corrida y la verificación
la detecta como diferencia.

Core no modifica el versionado: un bucket nuevo nace sin versionar y, si
alguien lo habilitó, la verificación falla con un mensaje accionable. S3 no
permite deshabilitar el versionado una vez activado (solo suspenderlo), así
que revertirlo es una decisión manual. La retención de la evidencia redactada
(`EVIDENCE_RETENTION_DAYS`) no es de Core: la aplica el sweeper de Alert.

### Configuración

| Variable | Valor por defecto | Descripción |
|---|---|---|
| `S3_REGION` | obligatoria | Región del bucket. Debe coincidir en Core, Alert e IA |
| `EVIDENCE_BUCKET` | obligatoria | Nombre del bucket. Es global en AWS: lleva un sufijo único, uno por entorno |
| `S3_TIMEOUT_MS` | `5000` | Timeout de cada operación contra S3 |
| `EVIDENCE_PENDING_DAYS` | `2` | Días de vida de los originales cifrados que nunca recibieron veredicto |
| `EVIDENCE_QUARANTINE_DAYS` | `15` | Días de vida de los originales de alertas RED (ventana de apelación) |

Las credenciales no son variables de este proyecto: se resuelven con la
cadena estándar del SDK de AWS (`AWS_ACCESS_KEY_ID` y `AWS_SECRET_ACCESS_KEY`
en desarrollo, rol IAM en producción). No hay `S3_ENDPOINT`: no existe un
emulador local, y desarrollo usa un bucket real de AWS propio del entorno.

### Desarrollo local

1. Copiar `.env.example` a `.env` y completar `EVIDENCE_BUCKET` (con sufijo
   único), `S3_REGION`, `AWS_ACCESS_KEY_ID` y `AWS_SECRET_ACCESS_KEY`. El
   `.env` está ignorado por git; las credenciales nunca van en el compose.
2. `docker compose up --build`. Core crea el bucket si no existe y le
   reaplica la configuración.

A diferencia del broker, el bucket persiste entre corridas:
`docker compose down -v` reconstruye la topología de RabbitMQ pero no toca
S3, y `cmd/reset` tampoco.

### Políticas IAM

Un usuario o rol IAM por servicio, con el mínimo de permisos. En los tres
ejemplos, reemplazar `EVIDENCE_BUCKET` por el nombre real del bucket.

**Core** — solo configura el bucket; sin acceso a objetos. `s3:ListBucket` lo
exige `HeadBucket`:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "CoreCreateBucket",
      "Effect": "Allow",
      "Action": "s3:CreateBucket",
      "Resource": "arn:aws:s3:::EVIDENCE_BUCKET"
    },
    {
      "Sid": "CoreConfigureBucket",
      "Effect": "Allow",
      "Action": [
        "s3:ListBucket",
        "s3:PutBucketPublicAccessBlock",
        "s3:GetBucketPublicAccessBlock",
        "s3:PutEncryptionConfiguration",
        "s3:GetEncryptionConfiguration",
        "s3:PutLifecycleConfiguration",
        "s3:GetLifecycleConfiguration",
        "s3:GetBucketVersioning"
      ],
      "Resource": "arn:aws:s3:::EVIDENCE_BUCKET"
    }
  ]
}
```

**Alert** — escribe, lee y borra los originales. `s3:ListBucket` con prefijo
`alerts/` evita que S3 responda 403 en lugar de `NoSuchKey` ante un objeto
inexistente:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "AlertObjects",
      "Effect": "Allow",
      "Action": [
        "s3:PutObject",
        "s3:GetObject",
        "s3:DeleteObject",
        "s3:PutObjectTagging"
      ],
      "Resource": "arn:aws:s3:::EVIDENCE_BUCKET/alerts/*"
    },
    {
      "Sid": "AlertList",
      "Effect": "Allow",
      "Action": "s3:ListBucket",
      "Resource": "arn:aws:s3:::EVIDENCE_BUCKET",
      "Condition": {
        "StringLike": { "s3:prefix": ["alerts/", "alerts/*"] }
      }
    }
  ]
}
```

**IA** — lee los originales y escribe las versiones redactadas:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "IAReadOriginals",
      "Effect": "Allow",
      "Action": "s3:GetObject",
      "Resource": "arn:aws:s3:::EVIDENCE_BUCKET/alerts/*/original/*"
    },
    {
      "Sid": "IAWriteRedacted",
      "Effect": "Allow",
      "Action": "s3:PutObject",
      "Resource": "arn:aws:s3:::EVIDENCE_BUCKET/alerts/*/redacted/*"
    }
  ]
}
```

### Prueba de integración contra AWS (opt-in)

`internal/storage` incluye una prueba con el build tag `aws` que corre el
aprovisionador dos veces seguidas contra el bucket real y verifica la
configuración tras cada corrida. No forma parte de `go test ./...`:

```
S3_REGION=us-east-1 EVIDENCE_BUCKET=vinculo-vial-evidence-dev-<sufijo> \
  go test -tags aws ./internal/storage/...
```

Requiere credenciales en la cadena del SDK y un bucket propio del entorno:
modifica la configuración del bucket que se le indique.

### Reset de topología (solo desarrollo local)

`cmd/reset` elimina la topología declarada por `cmd/core`, sin pasar por un
ciclo completo de `docker compose down`/`up`. Requiere `ENV=local`; con
cualquier otro valor, o sin definir la variable, termina con error antes de
conectar al broker.

```
ENV=local go run ./cmd/reset
```

No forma parte del arranque del sistema ni del `docker-compose.yml`: es una
herramienta manual, pensada para forzar una redeclaración de topología
durante el desarrollo sin reiniciar el contenedor del broker. Borra todas las
colas y exchanges de la plataforma, incluidos los flujos de evidencia de
US-08. Solo toca el broker: no se conecta a S3 ni modifica el bucket de
evidencia, que persiste entre corridas.

### Limpieza de exchange huérfano (legacy)

En versiones anteriores, Identity publicaba eventos en un exchange llamado
`vinculo-vial.events`, el cual ahora ha quedado huérfano tras la migración
al exchange unificado `vvi.topic.exchange`. Como este job de inicialización
no elimina recursos que no le pertenecen, la eliminación de `vinculo-vial.events`
en los brokers preexistentes debe realizarse por única vez de forma manual:
puede borrarse desde la UI de RabbitMQ Management o mediante la CLI de
administración del broker (`rabbitmqadmin delete exchange name=vinculo-vial.events`).

### Smoke test de enrutamiento (requiere broker real)

`internal/smoke` valida, contra un broker real, el ruteo que los tests
unitarios con canal simulado no pueden demostrar. Queda aislado con el build
tag `smoke` para no formar parte de `go test ./...` ni requerir un broker
disponible en la validación habitual. Cubre:

- **Ruteo por routing key:** un mensaje publicado en `vvi.topic.exchange` con
  `alert.created` o `user.sessions.revoked` llega a la cola de trabajo de
  Notification que la topología le asocia.
- **Wildcard de IA:** `ia.evidence.validated.green`, `.yellow` y `.red`
  llegan a `alert.ia-evidence-validated.q`, bindeada con
  `ia.evidence.validated.*`.
- **Reintento por nombre de cola:** con tres colas bindeadas a
  `alert.rejected` (la de Identity y dos de prueba), un `nack` sin requeue en
  una de las de prueba vuelve solo a esa cola, pasando por su `.retry`
  (`x-death` lo registra). Las otras, que ya hicieron `ack`, no reciben una
  segunda copia.
- **DLQ por nombre de cola:** un mensaje publicado en `vvi.dlq.exchange` con
  la routing key `W` queda solo en `W.dlq`.

Las colas de prueba (`smoke-probe-*`) son de la propia prueba: se declaran con
la misma función de flujo que usa la topología, con un `.retry` de 1 s para no
esperar `RETRY_TTL_MS`, y se borran al terminar.

```
RABBITMQ_URL=amqp://guest:guest@localhost:5672/ go test -tags smoke ./internal/smoke/...
```

La prueba declara la topología vigente de forma idempotente antes de
publicar, por lo que no depende de que `cmd/core` haya corrido antes. Antes
de publicar verifica que las colas de trabajo reales que usa no tengan
consumidores activos y falla si los hay: deben estar detenidos los consumidores
de `notification.alert-created.q`, `notification.sessions-revoked.q`,
`identity.alert-rejected.q` y `alert.ia-evidence-validated.q` (por ejemplo,
`docker stop vinculo-vial-notification`), porque competirían por el mismo
mensaje.

`docker compose up` requiere que exista de antemano la red externa
`vinculo-vial` (ver más abajo). Si no existe:

```
docker network create vinculo-vial
```

## Convención de nombres de infraestructura Docker

Para evitar colisiones de resolución DNS entre repositorios, toda la
infraestructura Docker de la plataforma sigue esta convención:

| Elemento | Regla | Aplicación en este repo |
|---|---|---|
| Contenedor principal | `container_name: vinculo-vial-[nombre-del-servicio]` | `vinculo-vial-core` |
| Contenedor secundario (DB o broker exclusivo) | `vinculo-vial-[nombre]-db` o `vinculo-vial-[tecnologia]` | `vinculo-vial-rabbitmq` |
| Red global | `vinculo-vial`, `external: true` | `core` y `rabbitmq` se conectan a ella |
| Red interna | `[nombre-del-servicio]-internal`, si se gestiona una base de datos privada | No aplica: RabbitMQ acá no es una base de datos privada de Core, es el broker compartido de todo el ecosistema, y debe permanecer alcanzable en la red global para que Alert y los demás servicios publiquen sobre la topología que Core declara |
| Volúmenes persistentes | `vinculo-vial-[nombre]-data` | No aplica: el broker de este repo no lleva volúmenes persistentes, por diseño, para que cada ciclo `down`/`up` ejercite la declaración de topología desde un estado vacío |

Cualquier servicio que necesite alcanzar RabbitMQ desde otro repositorio debe
direccionarlo por su `container_name` (`vinculo-vial-rabbitmq`), no por el
nombre de servicio de compose: ese alias es local a cada proyecto y puede
colisionar con el de otro repositorio en la red compartida.

## Release

El workflow `.github/workflows/publish.yml` construye la imagen y la publica
en Docker Hub al asignarse un tag `v*` (por ejemplo `v0.1.0`) sobre `main`.
Corre primero la suite de tests; si falla, no publica.

Requiere dos secrets configurados en el repositorio de GitHub:

| Secret | Contenido |
|---|---|
| `DOCKERHUB_USERNAME` | Usuario u organización de Docker Hub |
| `DOCKERHUB_TOKEN` | Access token de Docker Hub con permiso de push |

El versionado sigue SemVer. La imagen publicada queda etiquetada con el tag
de git tal cual (`v0.1.0`) y además como `latest`.

El workflow también admite disparo manual (`workflow_dispatch`) para validar
el pipeline sin necesidad de cortar un tag real.
