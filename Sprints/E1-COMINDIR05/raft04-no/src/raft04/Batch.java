
package raft04;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

// Mensagem LOTE: termo + lider + heartbeat + registos acumulados.
public final class Batch {

    private final long term;
    private final int leader;
    private final String address;
    private final int port;
    private final long hb;
    private final long lastSeq;
    private final long lastTerm;
    private final List<SensorRecord> records;

    public Batch(long term, int leader, String address, int port,
                 long hb, long lastSeq, long lastTerm,
                 List<SensorRecord> records) {

        this.term = term;
        this.leader = leader;
        this.address = address;
        this.port = port;
        this.hb = hb;
        this.lastSeq = lastSeq;
        this.lastTerm = lastTerm;

        this.records = Collections.unmodifiableList(
                new ArrayList<>(records)
        );
    }

    public long getTerm() {
        return term;
    }

    public int getLeader() {
        return leader;
    }

    public String getAddress() {
        return address;
    }

    public int getPort() {
        return port;
    }

    public long getHb() {
        return hb;
    }

    public long getLastSeq() {
        return lastSeq;
    }

    public long getLastTerm() {
        return lastTerm;
    }

    public List<SensorRecord> getRecords() {
        return records;
    }

    // Codifica o cabecalho e os registos.
    public String encode() {
        StringBuilder sb = new StringBuilder();

        sb.append("LOTE;")
                .append(term).append(';')
                .append(leader).append(';')
                .append(address).append(';')
                .append(port).append(';')
                .append(hb).append(';')
                .append(lastSeq).append(';')
                .append(lastTerm).append(';')
                .append(records.size());

        for (SensorRecord record : records) {
            sb.append('\n').append(record.toLine());
        }

        return sb.toString();
    }

    // Tamanho da mensagem codificada em UTF-8.
    public int encodedBytes() {
        return encode().getBytes(StandardCharsets.UTF_8).length;
    }

    // Interpreta uma mensagem LOTE da particao indicada.
    public static Batch decode(String msg, int partition) {

        if (msg == null || msg.isBlank()) {
            throw new IllegalArgumentException("LOTE vazio");
        }

        String[] lines = msg.trim().split("\\r?\\n");
        String[] h = lines[0].trim().split(";", -1);

        if (h.length != 9 || !h[0].equals("LOTE")) {
            throw new IllegalArgumentException(
                    "Cabecalho LOTE mal formado: " + lines[0]
            );
        }

        long term;
        int leader;
        String address;
        int port;
        long hb;
        long lastSeq;
        long lastTerm;
        int n;

        try {
            term = Long.parseLong(h[1].trim());
            leader = Integer.parseInt(h[2].trim());
            address = h[3].trim();
            port = Integer.parseInt(h[4].trim());
            hb = Long.parseLong(h[5].trim());
            lastSeq = Long.parseLong(h[6].trim());
            lastTerm = Long.parseLong(h[7].trim());
            n = Integer.parseInt(h[8].trim());

        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    "Cabecalho LOTE invalido: " + lines[0]
            );
        }

        if (term < 1 || leader < 1 || leader > Wire.GRUPO
                || address.isBlank()
                || port < 1 || port > 65535
                || hb < 1 || lastSeq < 0
                || lastTerm < 0 || n < 0) {

            throw new IllegalArgumentException(
                    "Cabecalho LOTE com valores invalidos: " + lines[0]
            );
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
                            + " registos, cabecalho indica n=" + n
            );
        }

        return new Batch(
                term, leader, address, port,
                hb, lastSeq, lastTerm, records
        );
    }
}
