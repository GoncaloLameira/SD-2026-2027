
package rep03;

import java.util.Arrays;

public class VectorClock {

    private final int[] clock;

    public VectorClock(int partitions) {
        if (partitions <= 0) {
            throw new IllegalArgumentException("Numero de particoes invalido");
        }
        clock = new int[partitions];
    }

    // Incrementa o relogio da particao indicada
    public synchronized void increment(int partition) {
        clock[partition]++;
    }

    // Atualiza o relogio com os valores recebidos
    public synchronized void merge(int[] received) {
        if (received.length != clock.length) {
            throw new IllegalArgumentException("Dimensao do vetor invalida");
        }

        for (int i = 0; i < clock.length; i++) {
            clock[i] = Math.max(clock[i], received[i]);
        }
    }

    // Devolve uma copia do relogio atual
    public synchronized int[] getClock() {
        return Arrays.copyOf(clock, clock.length);
    }

    // Verifica se este relogio aconteceu antes de outro
    public synchronized boolean happenedBefore(int[] other) {
        if (other.length != clock.length) {
            throw new IllegalArgumentException("Dimensao do vetor invalida");
        }

        boolean strictlyLess = false;

        for (int i = 0; i < clock.length; i++) {
            if (clock[i] > other[i]) {
                return false;
            }

            if (clock[i] < other[i]) {
                strictlyLess = true;
            }
        }

        return strictlyLess;
    }

    @Override
    public synchronized String toString() {
        return Arrays.toString(clock);
    }
}
