# vinculo-vial-core

Job de inicialización de infraestructura del ecosistema Vínculo Vial: declara,
de forma idempotente, la topología de RabbitMQ (exchanges, colas, bindings,
reintento y dead-letter) y los buckets de evidencia y de Audit en AWS S3, los
verifica y termina. No consume ni publica eventos de negocio, ni lee ni escribe
objetos.

## Requisitos

- Go 1.25+
- Docker y Docker Compose

## Comandos

```
go run ./cmd/core
go test ./...
go test -tags aws ./internal/storage/...      # opt-in, buckets de desarrollo reales
CGO_ENABLED=0 go build -o core ./cmd/core
docker compose up --build
```

`go run ./cmd/core` y `docker compose up` requieren la configuración de los
buckets (ver las secciones siguientes): sin `S3_REGION`, `EVIDENCE_BUCKET`,
`AUDIT_BUCKET` y credenciales AWS, el job termina con error.

## Colas de la plataforma

Cada flujo declara tres colas durables: `W` (trabajo, `<consumidor>.<evento>.q`),
`W.retry` y `W.dlq`. El retry y la DLQ se rutean por el nombre de `W`, no por
la routing key del evento. Todas las colas de trabajo se ligan a
`vvi.topic.exchange`.

| Cola de trabajo (`W`) | Binding | Productor → consumidor | CU |
|---|---|---|---|
| `notification.alert-created.q` | `alert.created` | Alert → Notification | existente |
| `notification.sessions-revoked.q` | `user.sessions.revoked` | Identity → Notification | existente |
| `ia.alert-evidence-uploaded.q` | `alert.evidence.uploaded` | Alert → IA | US-08 |
| `alert.ia-evidence-validated.q` | `ia.evidence.validated.*` | IA → Alert | US-08 |
| `identity.alert-rejected.q` | `alert.rejected` | Alert → Identity | US-08 |
| `identity.alert-reported.q` | `alert.reported` | Alert → Identity | US-08 |
| `alert.user-status-changed.q` | `user.status.changed` | Identity → Alert | US-08 |
| `notification.alert-rejected.q` | `alert.rejected` | Alert → Notification | US-09 |
| `notification.alert-expired.q` | `alert.expired` | Alert → Notification | US-09 |
| `notification.alert-seen.q` | `alert.seen` | Alert → Notification | US-09 |
| `alert.vehicle-pending-claimed.q` | `vehicle.pending.claimed` | Vehicle → Alert | US-10 |
| `notification.alert-pending-delivered.q` | `alert.pending.delivered` | Alert → Notification | US-10 |
| `alert.user-deleted.q` | `user.deleted` | Identity → Alert | US-11 |
| `vehicle.user-deleted.q` | `user.deleted` | Identity → Vehicle | US-11 |
| `audit.alert-recorded.q` | `audit.alert.recorded` | Alert → Audit | US-11 |
| `audit.alert-delivered.q` | `audit.alert.delivered` | Alert → Audit | US-11 |
| `audit.alert-discarded.q` | `audit.alert.discarded` | Alert → Audit | US-11 |

`vvi.unrouted.q` recibe, vía `alternate-exchange`, lo que no matchea ninguna
cola. `alert.rejected` y `user.deleted` tienen dos consumidores cada uno, cada
uno con su propia cola: un fallo en uno no reentrega al otro. Cada evento
`audit.alert.*` tiene un único consumidor, Audit, y su routing key coincide
con su `eventType`.

Las colas de US-09, US-10 y US-11 son aditivas: en un broker con estado no hay `406`, solo se
declaran las nuevas.

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

### Excepción a la directiva de circuit breaker

La sección 12.1 de las reglas de la plataforma exige un circuit breaker por
cada dependencia sincrónica. Core no lo implementa para S3, a propósito: esa
directiva apunta a servicios de larga vida que atienden requests, donde un
breaker evita que una dependencia degradada arrastre al resto. Core es un job
de inicialización: espera de forma acotada (30 s, con backoff exponencial y
jitter, sin reintentos del SDK y con un timeout por operación) y sale con
código distinto de 0 si S3 no responde. No hay requests que proteger ni un
estado que mantener entre llamadas.

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

1. Copiar `.env.example` a `.env` y completar `EVIDENCE_BUCKET` y
   `AUDIT_BUCKET` (con sufijo único cada uno), `S3_REGION`,
   `AWS_ACCESS_KEY_ID` y `AWS_SECRET_ACCESS_KEY`. El `.env` está ignorado por
   git; las credenciales nunca van en el compose.
2. `docker compose up --build`. Core crea los buckets si no existen y les
   reaplica la configuración.

A diferencia del broker, los buckets persisten entre corridas:
`docker compose down -v` reconstruye la topología de RabbitMQ pero no toca
S3, y `cmd/reset` tampoco.

### Políticas IAM

Un usuario o rol IAM por servicio, con el mínimo de permisos. En los tres
ejemplos, reemplazar `EVIDENCE_BUCKET` por el nombre real del bucket. Los
permisos de Core sobre el bucket de Audit están en su propia sección.

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

**Alert** — escribe, lee y borra los originales. Tiene además `s3:ListBucket`
sobre el bucket, **sin condición `s3:prefix`**: sin ese permiso, un objeto
inexistente devuelve 403 en lugar de `NoSuchKey`, y la condición de prefijo
no aplica a Get/Head, así que tampoco serviría. Exponer solo los nombres de
las claves es aceptable, porque contienen `alert_id`s y no datos de usuario:

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
      "Resource": "arn:aws:s3:::EVIDENCE_BUCKET"
    }
  ]
}
```

**IA** — lee los originales y escribe las versiones redactadas. Igual que
Alert, tiene `s3:ListBucket` sobre el bucket sin condición, por el mismo
motivo:

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
    },
    {
      "Sid": "IAList",
      "Effect": "Allow",
      "Action": "s3:ListBucket",
      "Resource": "arn:aws:s3:::EVIDENCE_BUCKET"
    }
  ]
}
```

### Prueba de integración contra AWS (opt-in)

`internal/storage` incluye una prueba con el build tag `aws` que corre el
aprovisionador dos veces seguidas contra cada bucket real (evidencia y Audit)
y verifica la configuración tras cada corrida. No forma parte de
`go test ./...`:

```
S3_REGION=us-east-1 \
  EVIDENCE_BUCKET=vinculo-vial-evidence-dev-<sufijo> \
  AUDIT_BUCKET=vinculo-vial-audit-dev-<sufijo> \
  go test -tags aws ./internal/storage/...
```

Requiere credenciales en la cadena del SDK y buckets propios del entorno:
modifica la configuración de los buckets que se le indiquen.

## Bucket de Audit (AWS S3)

Desde US-11, Core declara también el bucket donde Audit conserva, bloqueadas,
las copias de las fotos redactadas y el manifiesto de cada alerta entregada
(`records/{alert_id}/...`). Core sigue sin leer ni escribir objetos: solo
configura el bucket. Lo declara con el mismo aprovisionador y el mismo
verificador que el de evidencia, a partir de una segunda especificación
(`storage.AuditSpec`), y después del de evidencia en cada corrida.

| Propiedad | Valor |
|---|---|
| Acceso | Privado: Block Public Access con los cuatro indicadores en `true` |
| Cifrado | SSE-S3 (`AES256`) por defecto |
| Versionado y Object Lock | Se crean habilitados (`ObjectLockEnabledForBucket`); Object Lock activa el versionado |
| Retención por defecto | Ninguna: la fija Audit en cada objeto (`AUDIT_OBJECT_LOCK_MODE` y `RetainUntilDate`, variables de Audit y no de Core) |
| Lifecycle | Una regla sobre todo el bucket: `NoncurrentVersionExpiration` a 1 día y `ExpiredObjectDeleteMarker` |

El lifecycle limpia lo que Audit borra: como el bucket está versionado, cada
borrado deja versiones no vigentes y delete markers. S3 respeta la retención
vigente de cada versión, así que la regla nunca adelanta un borrado. Se
aplica completo en cada corrida, igual que el de evidencia.

**Object Lock solo se puede habilitar al crear el bucket.** Si `AUDIT_BUCKET`
ya existe sin Object Lock, Core no lo corrige: reaplica la configuración y la
verificación falla con el mensaje "el bucket existe sin Object Lock", y el job
termina con código distinto de 0. Core nunca borra ni recrea un bucket; la
salida es una decisión manual (vaciarlo y eliminarlo, o definir otro
`AUDIT_BUCKET`).

La verificación comprueba, además de Block Public Access, cifrado y
lifecycle:

- **Object Lock habilitado y sin retención por defecto.** Una retención por
  defecto configurada a mano también falla.
- **Versionado habilitado.** En el bucket de evidencia sigue siendo al
  revés: debe estar sin versionar.

### Configuración

| Variable | Valor por defecto | Descripción |
|---|---|---|
| `AUDIT_BUCKET` | obligatoria | Nombre del bucket de Audit. Global en AWS: lleva un sufijo único, uno por entorno (`vinculo-vial-audit-<entorno>-<sufijo>`). Distinto de `EVIDENCE_BUCKET`; debe coincidir con el `AUDIT_BUCKET` de Audit |

`S3_REGION` y `S3_TIMEOUT_MS` son los mismos que los del bucket de evidencia.

### Permisos IAM de Core

Para crear el bucket con Object Lock, AWS exige además de `s3:CreateBucket`
los permisos `s3:PutBucketVersioning` y `s3:PutBucketObjectLockConfiguration`.
Core no los usa en corridas posteriores, pero sin ellos el primer
`CreateBucket` falla. Reemplazar `AUDIT_BUCKET` por el nombre real:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "CoreCreateAuditBucket",
      "Effect": "Allow",
      "Action": [
        "s3:CreateBucket",
        "s3:PutBucketVersioning",
        "s3:PutBucketObjectLockConfiguration"
      ],
      "Resource": "arn:aws:s3:::AUDIT_BUCKET"
    },
    {
      "Sid": "CoreConfigureAuditBucket",
      "Effect": "Allow",
      "Action": [
        "s3:ListBucket",
        "s3:PutBucketPublicAccessBlock",
        "s3:GetBucketPublicAccessBlock",
        "s3:PutEncryptionConfiguration",
        "s3:GetEncryptionConfiguration",
        "s3:PutLifecycleConfiguration",
        "s3:GetLifecycleConfiguration",
        "s3:GetBucketVersioning",
        "s3:GetBucketObjectLockConfiguration"
      ],
      "Resource": "arn:aws:s3:::AUDIT_BUCKET"
    }
  ]
}
```

Sin acceso a objetos: el rol de Core no puede leer, escribir ni borrar nada
del bucket. Los permisos de Audit sobre los objetos (incluidos
`s3:PutObjectRetention` y `s3:PutObjectLegalHold`) son de su propio repo.

## Herramientas de desarrollo

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
colas y exchanges de la plataforma, incluidos los flujos de
US-08, US-09, US-10 y US-11 (baja de cuenta y Audit). Solo toca el broker: no se
conecta a S3 ni modifica los buckets de evidencia y de Audit, que persisten
entre corridas.

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
- **`user.deleted` (US-11):** el mensaje llega a `alert.user-deleted.q` y a
  `vehicle.user-deleted.q`. Con esas dos y dos colas de prueba bindeadas al
  mismo evento, un `nack` sin requeue en una de las de prueba vuelve solo a
  esa cola.
- **`audit.alert.*` (US-11):** `audit.alert.recorded`, `.delivered` y
  `.discarded` llegan cada uno únicamente a `audit.alert-recorded.q`,
  `audit.alert-delivered.q` y `audit.alert-discarded.q`, respectivamente: las
  otras dos colas de Audit no reciben copia.

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
`identity.alert-rejected.q`, `alert.ia-evidence-validated.q`,
`alert.user-deleted.q`, `vehicle.user-deleted.q`, `audit.alert-recorded.q`,
`audit.alert-delivered.q` y `audit.alert-discarded.q` (por ejemplo,
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
