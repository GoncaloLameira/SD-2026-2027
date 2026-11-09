package raft04;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public final class NodeLogger {

    private final int partition;
    private final Path eventFile;
    private final Path heartbeatFile;

    public NodeLogger(int partition, int nodeId) {
        this.partition = partition;

        this.eventFile = Path.of(
                "eventos-p" + partition + "-n" + nodeId + ".txt"
        );

        this.heartbeatFile = Path.of(
                "hb-p" + partition + "-n" + nodeId + ".txt"
        );
    }

    public synchronized void logEvent(String event, String detail) {

        String line = System.currentTimeMillis()
                + ";" + event
                + ";p" + partition
                + ";" + detail
                + System.lineSeparator();

        System.out.print(line);

        try {
            Files.writeString(
                    eventFile,
                    line,
                    StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.APPEND
            );
        } catch (IOException e) {
            System.err.println(
                    "Erro ao escrever eventos: " + e.getMessage()
            );
        }
    }

    public synchronized void saveHeartbeat(
            long term,
            long heartbeat,
            long lastSeq,
            long lastTerm,
            int count,
            int bytes
    ) throws IOException {

        String line = term + ";"
                + heartbeat + ";"
                + System.currentTimeMillis() + ";"
                + lastSeq + ";"
                + lastTerm + ";"
                + count + ";"
                + bytes
                + System.lineSeparator();

        Files.writeString(
                heartbeatFile,
                line,
                StandardCharsets.UTF_8,
                StandardOpenOption.CREATE,
                StandardOpenOption.APPEND
        );
    }
}