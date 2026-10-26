
package rep03;

public final class Partitioner {

    private final int n;

    public Partitioner(int n) {
        if (n < 1) {
            throw new IllegalArgumentException(
                    "Numero de particoes invalido: " + n
            );
        }

        this.n = n;
    }

    // Determina a particao de cada sensor
    public int of(String sensorId) {
        return Math.floorMod(sensorId.hashCode(), n);
    }

    // Devolve o numero total de particoes
    public int count() {
        return n;
    }
}
