package rep04;

import java.util.function.Consumer;

public final class HeartbeatMonitor {
    public enum State { DESCONHECIDO, ATIVO, SUSPEITO }

    private final int partition;
    private final long timeoutNanos;
    private final Consumer<String> log;

    private State state = State.DESCONHECIDO;
    private long lastNanos;   // instante local da ultima rececao
    private long inc = -1;    // incarnacao observada
    private long hb = 0;      // ultimo heartbeat observado

    public HeartbeatMonitor(int partition, long periodMs, int factor, Consumer<String> log) {
        this.partition = partition;
        this.timeoutNanos = periodMs * factor * 1_000_000L;
        this.log = log;
        this.lastNanos = System.nanoTime();
    }

    // thread de rececao: chegou um LOTE da particao
    public synchronized void onHeartbeat(long inc, long hb) {
        if (inc < this.inc) return;                    // incarnacao antiga
        if (inc == this.inc && hb <= this.hb) return;  // duplicado ou atrasado
        if (this.inc != -1 && inc != this.inc) {
            log.accept("REINICIO;p" + partition + ";inc=" + this.inc + "->" + inc);
        } else if (inc == this.inc && hb != this.hb + 1) {
            log.accept("HB_PERDIDO;p" + partition + ";esperado=" + (this.hb + 1) + ";recebido=" + hb);
        }
        if (state == State.SUSPEITO) {
            log.accept("RETOMA;p" + partition + ";inc=" + inc + ";hb=" + hb);
        }
        this.inc = inc;
        this.hb = hb;
        this.lastNanos = System.nanoTime();
        this.state = State.ATIVO;
    }

    // thread de verificacao: chamada a cada VERIFICACAO_MS
    public synchronized void check() {
        long elapsed = System.nanoTime() - lastNanos;
        if (state != State.SUSPEITO && elapsed > timeoutNanos) {
            state = State.SUSPEITO;
            log.accept("SUSPEITA;p" + partition + ";silencio_ms=" + elapsed / 1_000_000);
        }
    }

    public synchronized State state() {
        return state;
    }
}
