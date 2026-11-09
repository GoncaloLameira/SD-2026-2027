package raft04;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RecordFile {

    private final Path path;
    private final int partition;

    public RecordFile(String fileName) {
        Path pastaDados = Paths.get(
                System.getProperty("raft04.dataDir", ".")
        );
        try {
            Files.createDirectories(pastaDados);
            this.path = pastaDados.resolve(fileName);
        } catch (IOException e) {
            throw new RuntimeException(
                    "Erro ao criar a pasta de dados: " + pastaDados.toAbsolutePath(),
                    e
            );
        }
        String name = path.getFileName().toString();

        if (!name.matches("no-p\\d+-n\\d+\\.txt")) {
            throw new IllegalArgumentException(
                    "Nome de ficheiro invalido: " + name
            );
        }

        int inicio = "no-p".length();
        int fim = name.indexOf("-n");

        this.partition = Integer.parseInt(
                name.substring(inicio, fim)
        );

        if (partition < 1 || partition > Wire.PARTITIONS) {
            throw new IllegalArgumentException(
                    "Partição inválida: " + partition
            );
        }
    }

    public synchronized void append(SensorRecord record)
            throws IOException {

        if (record == null) {
            throw new IllegalArgumentException("Registo nulo.");
        }

        if (record.getPartition() != this.partition) {
            throw new IllegalArgumentException(
                    "Registo pertence a outra partição."
            );
        }

        long currentLast = lastSeq(partition);

        if (record.getSequence() != currentLast + 1) {
            throw new IOException(
                    "Sequência inválida. Esperada: "
                            + (currentLast + 1)
                            + ", recebida: "
                            + record.getSequence()
            );
        }

        Files.createDirectories(
                path.toAbsolutePath().getParent()
        );

        Files.writeString(
                path,
                record.toLine() + System.lineSeparator(),
                StandardCharsets.UTF_8,
                StandardOpenOption.CREATE,
                StandardOpenOption.APPEND
        );
    }

    private SensorRecord parseRecord(String line) {
        return SensorRecord.fromLine(line, partition);
    }

    public synchronized long lastSeq(int partition)
            throws IOException {

        checkPartition(partition);

        List<SensorRecord> records = readAll();

        long last = 0;

        for (SensorRecord record : records) {
            last = Math.max(last, record.getSequence());
        }

        return last;
    }

    public synchronized List<SensorRecord> range(
            int partition,
            long fromSeq,
            long toSeq
    ) throws IOException {

        checkPartition(partition);

        List<SensorRecord> result = new ArrayList<>();

        if (fromSeq > toSeq) {
            return result;
        }

        for (SensorRecord record : readAll()) {

            if (record.getSequence() >= fromSeq
                    && record.getSequence() <= toSeq) {

                result.add(record);
            }
        }

        result.sort(Comparator.comparingLong(
                SensorRecord::getSequence
        ));

        return result;
    }

    public synchronized boolean contains(
            int partition,
            long sequence
    ) throws IOException {

        checkPartition(partition);

        for (SensorRecord record : readAll()) {

            if (record.getSequence() == sequence) {
                return true;
            }
        }

        return false;
    }

    public synchronized long firstMissing(int partition)
            throws IOException {

        checkPartition(partition);

        List<SensorRecord> records = readAll();

        records.sort(Comparator.comparingLong(
                SensorRecord::getSequence
        ));

        long expected = 1;

        for (SensorRecord record : records) {

            if (record.getSequence() == expected) {
                expected++;
            } else if (record.getSequence() > expected) {
                return expected;
            }
        }

        return expected;
    }

    public synchronized SensorRecord lastRecord(int partition)
            throws IOException {

        checkPartition(partition);

        List<SensorRecord> records = readAll();

        return records.stream()
                .max(Comparator.comparingLong(
                        SensorRecord::getSequence
                ))
                .orElse(null);
    }

    private void checkPartition(int requestedPartition) {

        if (requestedPartition != this.partition) {
            throw new IllegalArgumentException(
                    "Partição pedida (" + requestedPartition
                            + ") diferente da partição do ficheiro ("
                            + this.partition + ")."
            );
        }
    }

    private List<SensorRecord> readAll() throws IOException {

        List<SensorRecord> records = new ArrayList<>();

        if (!Files.exists(path)) {
            return records;
        }

        List<String> lines = Files.readAllLines(
                path,
                StandardCharsets.UTF_8
        );

        Map<Long, SensorRecord> sequences = new HashMap<>();

        for (int i = 0; i < lines.size(); i++) {

            String line = lines.get(i);

            if (line == null || line.isBlank()) {
                continue;
            }

            SensorRecord record;

            try {
                record = parseRecord(line);
            } catch (RuntimeException e) {
                throw new IOException(
                        "Registo inválido na linha " + (i + 1)
                                + " do ficheiro " + path,
                        e
                );
            }

            SensorRecord previous = sequences.putIfAbsent(
                    record.getSequence(),
                    record
            );

            if (previous != null) {
                throw new IOException(
                        "Sequência duplicada no ficheiro "
                                + path + ": " + record.getSequence()
                );
            }

            records.add(record);
        }

        return records;
    }
}