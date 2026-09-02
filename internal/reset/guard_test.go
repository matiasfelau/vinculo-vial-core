package reset

import "testing"

func TestGuard_PermiteEntornoLocal(t *testing.T) {
	if err := Guard("local"); err != nil {
		t.Fatalf("error inesperado: %v", err)
	}
}

func TestGuard_RechazaProduccion(t *testing.T) {
	if err := Guard("production"); err == nil {
		t.Fatal("se esperaba error al intentar el reset fuera de un entorno local")
	}
}

func TestGuard_RechazaEntornoVacio(t *testing.T) {
	if err := Guard(""); err == nil {
		t.Fatal("se esperaba error al no definir ENV")
	}
}
