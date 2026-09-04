# vinculo-vial-core

Job de inicialización de infraestructura del ecosistema Vínculo Vial: declara,
de forma idempotente, la topología de RabbitMQ (exchanges, colas, bindings,
reintento y dead-letter) y termina. No consume ni publica eventos de negocio.

## Requisitos

- Go 1.25+
- Docker y Docker Compose

## Comandos

```
go run ./cmd/core
go test ./...
CGO_ENABLED=0 go build -o core ./cmd/core
docker compose up --build
```

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
durante el desarrollo sin reiniciar el contenedor del broker.

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
