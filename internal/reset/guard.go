// Package reset elimina la topología declarada por este job. Queda
// restringido a entornos locales: Guard exige ENV=local antes de permitir
// cualquier operación destructiva contra el broker.
package reset

import "fmt"

// localEnv es el único valor de ENV que habilita el reset. Se implementa
// como lista de permitidos, no de bloqueados: un guard que solo rechaza
// "production" dejaría pasar por omisión cualquier entorno futuro no
// contemplado (staging, homologación); exigir "local" explícito falla
// cerrado por defecto.
const localEnv = "local"

// Guard rechaza el reset fuera de un entorno local explícito.
func Guard(env string) error {
	if env != localEnv {
		return fmt.Errorf("reset: operación restringida a ENV=%q, se recibió %q", localEnv, env)
	}
	return nil
}
