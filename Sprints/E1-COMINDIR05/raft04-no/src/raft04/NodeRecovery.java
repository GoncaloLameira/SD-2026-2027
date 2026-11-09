package raft04;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;

public final class NodeRecovery {

    private final int partition;
    private final int nodeId;

    private final RecordFile recordFile;
    private final RaftState raftState;

    private final BooleanSupplier isolated;
    private final BiConsumer<String, String> logger;

    private final Consumer<SensorRecord> updateClocks;
    private final Runnable resetClocks;
    private final Consumer<SensorRecord> restoreClocks;

    public NodeRecovery(
            int partition,
            int nodeId,
            RecordFile recordFile,
            RaftState raftState,
            BooleanSupplier isolated,
            BiConsumer<String, String> logger,
            Consumer<SensorRecord> updateClocks,
            Runnable resetClocks,
            Consumer<SensorRecord> restoreClocks
    ) {
        this.partition = partition;
        this.nodeId = nodeId;
        this.recordFile = recordFile;
        this.raftState = raftState;
        this.isolated = isolated;
        this.logger = logger;
        this.updateClocks = updateClocks;
        this.resetClocks = resetClocks;
        this.restoreClocks = restoreClocks;
    }

    // ---------------------------------------------------------
    // OBTENÇÃO DE REGISTOS ATRAVÉS DE TCP
    // ---------------------------------------------------------

    private List<SensorRecord> fetchRecords(
            String host,
            int port,
            long from,
            long to
    ) throws IOException {

        return fetchRecords(host, port, from, to, -1);
    }

    private List<SensorRecord> fetchRecords(
            String host,
            int port,
            long from,
            long to,
            long expectedTerm
    ) throws IOException {

        List<SensorRecord> result = new ArrayList<>();

        if (from > to) {
            return result;
        }

        if (isolated.getAsBoolean()) {
            throw new IOException("Nó isolado.");
        }

        try (Socket socket = new Socket()) {

            socket.connect(
                    new InetSocketAddress(host, port),
                    Wire.T_TCP_MS
            );

            socket.setSoTimeout(Wire.T_TCP_MS);

            BufferedReader in = new BufferedReader(
                    new InputStreamReader(
                            socket.getInputStream(),
                            StandardCharsets.UTF_8
                    )
            );

            PrintWriter out = new PrintWriter(
                    new OutputStreamWriter(
                            socket.getOutputStream(),
                            StandardCharsets.UTF_8
                    ),
                    true
            );

            if (expectedTerm >= 0) {
                out.println(
                        "PEDIDO " + from + " " + to + " " + expectedTerm
                );
            } else {
                out.println("PEDIDO " + from + " " + to);
            }

            long expected = from;
            boolean receivedEnd = false;

            String line;

            while ((line = in.readLine()) != null) {

                if (isolated.getAsBoolean()) {
                    throw new IOException(
                            "Nó isolado durante a transferência."
                    );
                }

                if (line.startsWith("ERRO")) {
                    throw new IOException(line);
                }

                if (line.startsWith("FIM")) {

                    try {
                        if (expectedTerm >= 0) {

                            String[] fields = line.split(";");

                            if (fields.length != 2
                                    || Long.parseLong(fields[1]) != expectedTerm) {

                                throw new IOException(
                                        "O líder mudou de termo durante a transferência."
                                );
                            }

                        } else if (!"FIM".equals(line)) {

                            throw new IOException(
                                    "Resposta FIM inválida."
                            );
                        }

                    } catch (NumberFormatException e) {
                        throw new IOException(
                                "Termo inválido na resposta FIM.", e
                        );
                    }

                    receivedEnd = true;
                    break;
                }

                SensorRecord record;

                try {
                    record = SensorRecord.fromLine(line, partition);
                } catch (RuntimeException e) {
                    throw new IOException("Registo TCP inválido.", e);
                }

                if (record.getSequence() != expected
                        || record.getSequence() > to) {

                    throw new IOException(
                            "Sequência inesperada: " + record.getSequence()
                    );
                }

                result.add(record);
                expected++;
            }

            if (!receivedEnd || expected != to + 1) {
                throw new IOException("Transferência incompleta.");
            }
        }

        return result;
    }

    // ---------------------------------------------------------
    // RECUPERAÇÃO DE REGISTOS EM FALTA
    // ---------------------------------------------------------

    public boolean recoverRecords(
            String host,
            int port,
            long from,
            long to
    ) {

        if (isolated.getAsBoolean()) {
            logger.accept("RECUPERACAO_FALHOU", "motivo=no_isolado");
            return false;
        }

        if (from > to) {
            return true;
        }

        System.out.println(
                "A pedir recuperação TCP a " + host + ":" + port
                        + " | PEDIDO " + from + " " + to
        );

        try {

            List<SensorRecord> records =
                    fetchRecords(host, port, from, to);

            long expected = from;

            for (SensorRecord record : records) {

                if (isolated.getAsBoolean()) {
                    throw new IOException(
                            "Nó isolado durante a recuperação."
                    );
                }

                long currentLast = recordFile.lastSeq(partition);

                if (record.getSequence() != expected
                        || record.getSequence() != currentLast + 1) {

                    throw new IOException(
                            "A recuperação não respeita a ordem local."
                    );
                }

                recordFile.append(record);
                updateClocks.accept(record);

                expected++;
            }

            logger.accept(
                    "RECUPERACAO",
                    "de=" + from
                            + ";ate=" + to
                            + ";recuperados=" + records.size()
            );

            System.out.println(
                    "Recuperação TCP concluída: "
                            + records.size() + " registos."
            );

            return true;

        } catch (IOException e) {

            System.out.println(
                    "Erro na recuperação TCP: " + e.getMessage()
            );

            logger.accept(
                    "RECUPERACAO_FALHOU",
                    "de=" + from
                            + ";ate=" + to
                            + ";erro=" + e.getMessage()
            );

            return false;
        }
    }

    // ---------------------------------------------------------
    // DETEÇÃO DE DIVERGÊNCIA
    // ---------------------------------------------------------

    private long findFirstDivergence(
            List<SensorRecord> localRecords,
            List<SensorRecord> leaderRecords,
            long firstSequence
    ) {

        int commonSize = Math.min(
                localRecords.size(),
                leaderRecords.size()
        );

        for (int i = 0; i < commonSize; i++) {

            SensorRecord local = localRecords.get(i);
            SensorRecord leader = leaderRecords.get(i);

            if (local.getSequence() != leader.getSequence()
                    || local.getTerm() != leader.getTerm()
                    || !local.toLine().equals(leader.toLine())) {

                return firstSequence + i;
            }
        }

        if (localRecords.size() != leaderRecords.size()) {
            return firstSequence + commonSize;
        }

        return -1;
    }

    // ---------------------------------------------------------
    // RESOLUÇÃO DE DIVERGÊNCIA POR BLOCOS DE TERMOS
    // ---------------------------------------------------------

    public boolean resolveDivergence(
            String host,
            int port,
            long leaderLastSeq,
            int expectedLeader,
            long expectedTerm
    ) throws IOException {

        if (isolated.getAsBoolean()) {
            throw new IOException("Nó isolado.");
        }

        if (raftState.term() != expectedTerm
                || raftState.leader() != expectedLeader) {

            throw new IOException(
                    "O líder ou o termo mudou antes da recuperação."
            );
        }

        Path localPath = Path.of(
                "no-p" + partition + "-n" + nodeId + ".txt"
        );

        List<SensorRecord> localRecords = new ArrayList<>();

        if (Files.exists(localPath)) {

            for (String line : Files.readAllLines(
                    localPath,
                    StandardCharsets.UTF_8)) {

                if (!line.isBlank()) {
                    localRecords.add(
                            SensorRecord.fromLine(line, partition)
                    );
                }
            }
        }

        int blockStart = Math.max(0, localRecords.size() - 1);

        if (!localRecords.isEmpty()) {

            long lastTerm = localRecords.get(blockStart).getTerm();

            while (blockStart > 0
                    && localRecords.get(blockStart - 1).getTerm() == lastTerm) {

                blockStart--;
            }
        }

        long firstDifference;
        List<SensorRecord> leaderRecords;

        while (true) {

            if (isolated.getAsBoolean()) {
                throw new IOException(
                        "Nó isolado durante a recuperação."
                );
            }

            long from = localRecords.isEmpty()
                    ? 1
                    : localRecords.get(blockStart).getSequence();

            if (from <= leaderLastSeq) {

                leaderRecords = fetchRecords(
                        host,
                        port,
                        from,
                        leaderLastSeq,
                        expectedTerm
                );

            } else {
                leaderRecords = new ArrayList<>();
            }

            List<SensorRecord> localSuffix = new ArrayList<>(
                    localRecords.subList(
                            blockStart,
                            localRecords.size()
                    )
            );

            firstDifference = findFirstDivergence(
                    localSuffix,
                    leaderRecords,
                    from
            );

            if (firstDifference != -1) {
                break;
            }

            if (blockStart == 0 || localRecords.isEmpty()) {
                return false;
            }

            int previousBlockStart = blockStart - 1;
            long previousTerm =
                    localRecords.get(previousBlockStart).getTerm();

            while (previousBlockStart > 0
                    && localRecords.get(previousBlockStart - 1).getTerm()
                    == previousTerm) {

                previousBlockStart--;
            }

            blockStart = previousBlockStart;
        }

        if (raftState.term() != expectedTerm
                || raftState.leader() != expectedLeader) {

            throw new IOException(
                    "O líder ou o termo mudou durante a recuperação."
            );
        }

        List<SensorRecord> correctedRecords = new ArrayList<>();
        List<Long> discardedTerms = new ArrayList<>();

        int discarded = 0;

        for (SensorRecord record : localRecords) {

            if (record.getSequence() < firstDifference) {
                correctedRecords.add(record);
            } else {
                discarded++;
                discardedTerms.add(record.getTerm());
            }
        }

        for (SensorRecord record : leaderRecords) {

            if (record.getSequence() >= firstDifference) {
                correctedRecords.add(record);
            }
        }

        Path temporaryFile = Path.of(
                localPath.toString() + ".tmp"
        );

        try {

            List<String> correctedLines = new ArrayList<>();

            for (SensorRecord record : correctedRecords) {
                correctedLines.add(record.toLine());
            }

            String content = correctedLines.isEmpty()
                    ? ""
                    : String.join(
                    System.lineSeparator(),
                    correctedLines
            ) + System.lineSeparator();

            Files.writeString(
                    temporaryFile,
                    content,
                    StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING,
                    StandardOpenOption.WRITE
            );

            if (isolated.getAsBoolean()) {
                throw new IOException(
                        "Nó isolado. Substituição cancelada."
                );
            }

            if (raftState.term() != expectedTerm
                    || raftState.leader() != expectedLeader) {

                throw new IOException(
                        "O líder mudou. A substituição foi cancelada."
                );
            }

            try {

                Files.move(
                        temporaryFile,
                        localPath,
                        StandardCopyOption.REPLACE_EXISTING,
                        StandardCopyOption.ATOMIC_MOVE
                );

            } catch (IOException atomicMoveError) {

                Files.move(
                        temporaryFile,
                        localPath,
                        StandardCopyOption.REPLACE_EXISTING
                );
            }

        } finally {
            Files.deleteIfExists(temporaryFile);
        }

        resetClocks.run();

        for (SensorRecord record : correctedRecords) {
            restoreClocks.accept(record);
        }

        String terms = discardedTerms.isEmpty()
                ? "nenhum"
                : discardedTerms.toString();

        logger.accept(
                "TRUNCADO",
                "de=" + firstDifference
                        + ";descartados=" + discarded
                        + ";termos=" + terms
        );

        System.out.println(
                "DIVERGÊNCIA RESOLVIDA | primeira sequência: "
                        + firstDifference
                        + " | registos descartados: " + discarded
                        + " | termos: " + terms
        );

        return true;
    }
}