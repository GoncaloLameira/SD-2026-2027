
package rep03;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.List;

public class RecordFile {

    private final Path path;
    private final int partition;

    public RecordFile(String fileName) {
        this.path = Paths.get(fileName);

        String name = path.getFileName().toString();

        if (!name.matches("lider-p\\d+\\.txt")) {
            throw new IllegalArgumentException(
                    "Nome de ficheiro invalido: " + name);
        }

        String numero = name.substring(
                "lider-p".length(), name.length() - ".txt".length());

        this.partition = Integer.parseInt(numero);
    }

    // Guarda um registo no fim do ficheiro
    public synchronized void append(SensorRecord record)
            throws IOException {

        Files.writeString(
                path,
                record.toLine() + System.lineSeparator(),
                StandardCharsets.UTF_8,
                StandardOpenOption.CREATE,
                StandardOpenOption.APPEND
        );
    }

    // Converte um registo antigo ou novo para SensorRecord
    private SensorRecord parseRecord(String line) {

        line = line.trim();

// Formato antigo:
// particao;seq;sensor;temperatura;L;V
//
// Formato novo:
// seq;sensor;temperatura;instante;L;V

        if (!line.startsWith("REG |")) {

            String[] campos = line.split(";", -1);

            if (campos.length != 6) {
                throw new IllegalArgumentException(
                        "Registo mal formado: " + line);
            }

            // No formato antigo, o segundo campo e a sequencia.
            // No formato novo, o segundo campo e o sensor.
            if (campos[1].trim().matches("\\d+")) {
                return SensorRecord.fromLine(line);
            }

            return SensorRecord.fromLine(line, partition);
        }

        // Formato antigo:
        // REG | p=0 | seq=10 | sensor=sensor10
        // | temp=25.5 | L=1 | V=[1, 0]

        int partition = -1;
        long sequence = -1;
        String sensor = null;
        Double temperature = null;
        int lamport = 0;
        int[] vector = null;

        String[] parts = line.split("\\|");

        for (String part : parts) {

            String value = part.trim();

            if (value.startsWith("p=")) {
                partition = Integer.parseInt(
                        value.substring(2).trim());

            } else if (value.startsWith("seq=")) {
                sequence = Long.parseLong(
                        value.substring(4).trim());

            } else if (value.startsWith("sensor=")) {
                sensor = value.substring(7).trim();

            } else if (value.startsWith("temp=")) {
                temperature = Double.parseDouble(
                        value.substring(5).trim());

            } else if (value.startsWith("L=")) {
                lamport = Integer.parseInt(
                        value.substring(2).trim());

            } else if (value.startsWith("V=")) {

                String texto = value.substring(2)
                        .replace("[", "")
                        .replace("]", "")
                        .trim();

                if (texto.isEmpty()) {
                    vector = new int[0];
                } else {

                    String[] numbers = texto.split(",");
                    vector = new int[numbers.length];

                    for (int i = 0; i < numbers.length; i++) {
                        vector[i] = Integer.parseInt(
                                numbers[i].trim());
                    }
                }
            }
        }

        if (partition < 0 || sequence < 1
                || sensor == null || temperature == null
                || vector == null) {

            throw new IllegalArgumentException(
                    "Registo antigo invalido: " + line);
        }

        return new SensorRecord(
                partition,
                sequence,
                sensor,
                temperature,
                lamport,
                vector
        );
    }

    // Devolve a ultima sequencia de uma particao
    public synchronized long lastSeq(int partition)
            throws IOException {

        if (!Files.exists(path)) {
            return 0;
        }

        long last = 0;

        for (String line : Files.readAllLines(
                path, StandardCharsets.UTF_8)) {

            if (line.isBlank()) {
                continue;
            }

            SensorRecord record = parseRecord(line);

            if (record.getPartition() == partition) {
                last = Math.max(
                        last, record.getSequence());
            }
        }

        return last;
    }

    // Recupera registos num intervalo de sequencias
    public synchronized List<SensorRecord> range(
            int partition, long from, long to)
            throws IOException {

        List<SensorRecord> records = new ArrayList<>();

        if (!Files.exists(path)) {
            return records;
        }

        for (String line : Files.readAllLines(
                path, StandardCharsets.UTF_8)) {

            if (line.isBlank()) {
                continue;
            }

            SensorRecord record = parseRecord(line);

            if (record.getPartition() == partition
                    && record.getSequence() >= from
                    && record.getSequence() <= to) {

                records.add(record);
            }
        }

        // Garante a ordem correta de recuperacao
        records.sort(
                (a, b) -> Long.compare(
                        a.getSequence(),
                        b.getSequence())
        );

        return records;
    }

    // Verifica se uma sequencia ja esta guardada
    public synchronized boolean contains(
            int partition, long sequence)
            throws IOException {

        if (!Files.exists(path)) {
            return false;
        }

        for (String line : Files.readAllLines(
                path, StandardCharsets.UTF_8)) {

            if (line.isBlank()) {
                continue;
            }

            SensorRecord record = parseRecord(line);

            if (record.getPartition() == partition
                    && record.getSequence() == sequence) {
                return true;
            }
        }

        return false;
    }

    // Procura a primeira sequencia em falta
    public synchronized long firstMissing(
            int partition, long limit)
            throws IOException {

        for (long seq = 1; seq <= limit; seq++) {

            if (!contains(partition, seq)) {
                return seq;
            }
        }

        return limit + 1;
    }

    // Devolve o registo com a maior sequencia de uma particao
    public synchronized SensorRecord lastRecord(int partition)
            throws IOException {

        if (!Files.exists(path)) {
            return null;
        }

        SensorRecord last = null;

        for (String line : Files.readAllLines(
                path, StandardCharsets.UTF_8)) {

            if (line.isBlank()) {
                continue;
            }

            SensorRecord record = parseRecord(line);

            if (record.getPartition() == partition
                    && (last == null
                    || record.getSequence() > last.getSequence())) {

                last = record;
            }
        }

        return last;
    }

}
