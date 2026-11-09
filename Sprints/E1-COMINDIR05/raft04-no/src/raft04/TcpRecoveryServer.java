package raft04;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.BooleanSupplier;

public final class TcpRecoveryServer {

    private final int partition;
    private final int nodeId;
    private final RecordFile recordFile;
    private final RaftState raftState;
    private final BooleanSupplier isolated;
    private final BiConsumer<String, String> logger;

    public TcpRecoveryServer(
            int partition,
            int nodeId,
            RecordFile recordFile,
            RaftState raftState,
            BooleanSupplier isolated,
            BiConsumer<String, String> logger
    ) {
        this.partition = partition;
        this.nodeId = nodeId;
        this.recordFile = recordFile;
        this.raftState = raftState;
        this.isolated = isolated;
        this.logger = logger;
    }

    public void start() {
        Thread thread = new Thread(() -> {
            int port = Wire.nodePort(partition, nodeId);

            try (ServerSocket server = new ServerSocket(port)) {

                System.out.println(
                        "Servidor TCP ativo | partição " + partition
                                + " | nó " + nodeId
                                + " | porta " + port
                );

                while (!Thread.currentThread().isInterrupted()) {
                    try (Socket socket = server.accept()) {

                        if (isolated.getAsBoolean()) {
                            continue;
                        }

                        processRequest(socket);

                    } catch (IOException e) {
                        System.err.println(
                                "Erro num pedido TCP: " + e.getMessage()
                        );
                    }
                }

            } catch (IOException e) {
                System.err.println(
                        "Erro no servidor TCP: " + e.getMessage()
                );
            }
        }, "tcp-server-p" + partition + "-n" + nodeId);

        thread.setDaemon(true);
        thread.start();
    }

    private void processRequest(Socket socket) throws IOException {
        socket.setSoTimeout(Wire.T_TCP_MS);
        if (isolated.getAsBoolean()) {
            return;
        }
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

        String pedido = in.readLine();

        if (pedido == null) {
            return;
        }

        String[] partes = pedido.trim().split("\\s+");

        if ((partes.length != 3 && partes.length != 4)
                || !"PEDIDO".equals(partes[0])) {
            out.println("ERRO pedido invalido");
            return;
        }

        long from;
        long to;
        long expectedTerm = -1;

        try {
            from = Long.parseLong(partes[1]);
            to = Long.parseLong(partes[2]);

            if (partes.length == 4) {
                expectedTerm = Long.parseLong(partes[3]);
            }

        } catch (NumberFormatException e) {
            out.println("ERRO parametros invalidos");
            return;
        }

        if (from < 1 || from > to) {
            out.println("ERRO intervalo invalido");
            return;
        }

        if (expectedTerm >= 0
                && raftState.termIfLeader() != expectedTerm) {
            out.println("ERRO lider ou termo alterado");
            return;
        }

        List<SensorRecord> records =
                recordFile.range(partition, from, to);

        for (SensorRecord record : records) {
            if (isolated.getAsBoolean()) {
                return;
            }

            out.println(record.toLine());
        }
        if (expectedTerm >= 0
                && raftState.termIfLeader() != expectedTerm) {
            out.println("ERRO lider ou termo alterado");
            return;
        }
        if (isolated.getAsBoolean()) {
            return;
        }
        out.println(
                expectedTerm >= 0
                        ? "FIM;" + expectedTerm
                        : "FIM"
        );

        logger.accept(
                "TCP_SERVIDO",
                "de=" + from
                        + ";ate=" + to
                        + ";n=" + records.size()
        );
    }
}