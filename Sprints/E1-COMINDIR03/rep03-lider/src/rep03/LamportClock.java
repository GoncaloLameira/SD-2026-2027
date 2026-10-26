
package rep03;

public class LamportClock {

    private int clock = 0;

    // Incrementa o relogio quando ocorre um evento local
    public synchronized int tick() {
        clock++;
        return clock;
    }

    // Atualiza o relogio quando recebe uma mensagem
    public synchronized int update(int receivedClock) {
        clock = Math.max(clock, receivedClock) + 1;
        return clock;
    }

    // Devolve o valor atual do relogio
    public synchronized int getClock() {
        return clock;
    }
    // Restaura o relogio a partir de um valor guardado
    public synchronized void restore(int savedClock) {
        if (savedClock < 0) {
            throw new IllegalArgumentException(
                    "Valor do relogio invalido"
            );
        }

        clock = Math.max(clock, savedClock);
    }
}
