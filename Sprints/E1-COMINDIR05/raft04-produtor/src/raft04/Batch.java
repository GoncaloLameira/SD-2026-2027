package raft04;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

// Mensagem LOTE: heartbeat + registos acumulados no intervalo.
//
// LOTE;<endereco>;<porto>;<inc>;<hb>;<ultimoSeq>;<n>
// <seq>;<sensor>;<temperatura>;<instante>;<L>;<V>      (n linhas)
public final class Batch {

    private final String address;
    private final int port;
    private final long inc;
    private final long hb;
    private final long lastSeq;
    private final List<SensorRecord> records;

    public Batch(String address, int port, long inc, long hb,
                 long lastSeq, List<SensorRecord> records) {
        this.address = address;
        this.port = port;
        this.inc = inc;
        this.hb = hb;
        this.lastSeq = lastSeq;
        this.records = Collections.unmodifiableList(
                new ArrayList<>(records));
    }

    public String getAddress() {
        return address;
    }

    public int getPort() {
        return port;
    }

    public long getInc() {
        return inc;
    }

    public long getHb() {
        return hb;
    }

    public long getLastSeq() {
        return lastSeq;
    }

    public List<SensorRecord> getRecords() {
        return records;
    }

    // Produz a mensagem LOTE (cabecalho + n linhas de registo)
    public String encode() {
        StringBuilder sb = new StringBuilder();

        sb.append("LOTE;")
                .append(address).append(';')
                .append(port).append(';')
                .append(inc).append(';')
                .append(hb).append(';')
                .append(lastSeq).append(';')
                .append(records.size());

        for (SensorRecord record : records) {
            sb.append('\n').append(record.toLine());
        }

        return sb.toString();
    }

    // Tamanho em bytes da mensagem codificada (UTF-8)
    public int encodedBytes() {
        return encode().getBytes(StandardCharsets.UTF_8).length;
    }

    // Interpreta uma mensagem LOTE da particao indicada.
    // A particao nao viaja nas linhas de registo; vem do topico.
    // Valida que o numero de linhas de registo coincide com o n do cabecalho.
    public static Batch decode(String msg, int partition) {
        if (msg == null || msg.isBlank()) {
            throw new IllegalArgumentException("LOTE vazio");
        }

        String[] lines = msg.trim().split("\\r?\\n");
        String[] h = lines[0].trim().split(";", -1);

        if (h.length != 7 || !h[0].equals("LOTE")) {
            throw new IllegalArgumentException(
                    "Cabecalho LOTE mal formado: " + lines[0]);
        }

        String address = h[1].trim();
        int port;
        long inc;
        long hb;
        long lastSeq;
        int n;

        try {
            port = Integer.parseInt(h[2].trim());
            inc = Long.parseLong(h[3].trim());
            hb = Long.parseLong(h[4].trim());
            lastSeq = Long.parseLong(h[5].trim());
            n = Integer.parseInt(h[6].trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    "Cabecalho LOTE invalido: " + lines[0]);
        }

        if (n < 0 || hb < 1 || lastSeq < 0) {
            throw new IllegalArgumentException(
                    "Cabecalho LOTE com valores invalidos: " + lines[0]);
        }

        List<SensorRecord> records = new ArrayList<>();

        for (int i = 1; i < lines.length; i++) {
            if (lines[i].isBlank()) {
                continue;
            }
            records.add(SensorRecord.fromLine(lines[i], partition));
        }

        if (records.size() != n) {
            throw new IllegalArgumentException(
                    "LOTE com " + records.size()
                            + " registos, cabecalho indica n=" + n);
        }

        return new Batch(address, port, inc, hb, lastSeq, records);
    }
}
