
package tpf;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.List;

public final class RegistoLog {

    private static final Path LOG = Path.of("registos.log");
    private static final Path EVENTOS =
            Path.of("eventos-lider.txt");

    private final ChunkManager chunkManager;
    private long ultimoLsn;

    // Mantem compatibilidade com o LeaderBot atual.
    public RegistoLog() throws IOException {
        this(null);
    }

    // Novo construtor: permite associar um ChunkManager.
    public RegistoLog(ChunkManager chunkManager)
            throws IOException {

        this.chunkManager = chunkManager;
        this.ultimoLsn = recuperarUltimoLsn();

        System.out.println(
                "RegistoLog iniciado. Ultimo LSN: " + ultimoLsn
        );
    }

    public synchronized Registo registar(
            String autor,
            String canal,
            String texto
    ) throws IOException {

        long proximoLsn = ultimoLsn + 1;

        Registo registo = new Registo(
                proximoLsn,
                System.currentTimeMillis(),
                autor,
                canal,
                texto
        );

        byte[] dados = (registo.toLine() + "\n")
                .getBytes(StandardCharsets.UTF_8);

        // 1. Guarda primeiro o registo no log persistente.
        try (FileChannel channel = FileChannel.open(
                LOG,
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE,
                StandardOpenOption.APPEND
        )) {

            ByteBuffer buffer = ByteBuffer.wrap(dados);

            while (buffer.hasRemaining()) {
                channel.write(buffer);
            }

            channel.force(true);
        }

        // O registo ja foi persistido.
        ultimoLsn = proximoLsn;

        // 2. Regista o evento sem invalidar uma escrita
        // que ja foi concluida no registos.log.
        try {
            registarEvento(
                    "REGISTO",
                    "LSN=" + proximoLsn
                            + ";autor=" + autor
                            + ";canal=" + canal
            );

        } catch (IOException e) {
            System.err.println(
                    "AVISO: registo persistido, mas falhou "
                            + "o log de eventos: " + e.getMessage()
            );
        }

        // 3. Entrega o registo ao gestor dos chunks.
        if (chunkManager != null) {
            try {
                chunkManager.adicionar(registo);

            } catch (IOException | RuntimeException e) {
                // O registo continua guardado no registos.log.
                // A recuperacao para chunks sera tratada
                // posteriormente.
                System.err.println(
                        "AVISO: LSN=" + proximoLsn
                                + " persistido, mas falhou "
                                + "o encaminhamento para chunks: "
                                + e.getMessage()
                );
            }
        }

        return registo;
    }

    public synchronized long getUltimoLsn() {
        return ultimoLsn;
    }

    private long recuperarUltimoLsn() throws IOException {

        if (!Files.exists(LOG)) {
            return 0;
        }

        List<String> linhas = Files.readAllLines(
                LOG,
                StandardCharsets.UTF_8
        );

        long ultimo = 0;

        for (String linha : linhas) {

            if (linha.isBlank()) {
                continue;
            }

            Registo registo;

            try {
                registo = Registo.fromLine(linha);

            } catch (RuntimeException e) {
                throw new IOException(
                        "Registo invalido no ficheiro de log.",
                        e
                );
            }

            if (registo.lsn() != ultimo + 1) {
                throw new IOException(
                        "Sequencia LSN invalida. Esperado: "
                                + (ultimo + 1)
                                + ", encontrado: "
                                + registo.lsn()
                );
            }

            ultimo = registo.lsn();
        }

        return ultimo;
    }

    private void registarEvento(
            String evento,
            String detalhe
    ) throws IOException {

        String linha = Instant.now().toEpochMilli()
                + ";" + evento
                + ";" + detalhe
                + "\n";

        byte[] dados = linha.getBytes(
                StandardCharsets.UTF_8
        );

        try (FileChannel channel = FileChannel.open(
                EVENTOS,
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE,
                StandardOpenOption.APPEND
        )) {

            ByteBuffer buffer = ByteBuffer.wrap(dados);

            while (buffer.hasRemaining()) {
                channel.write(buffer);
            }

            channel.force(true);
        }
    }

    public synchronized List<Registo> lerTodos()
            throws IOException {

        if (!Files.exists(LOG)) {
            return List.of();
        }

        List<Registo> resultado = new java.util.ArrayList<>();

        long lsnEsperado = 1;

        for (String linha : Files.readAllLines(
                LOG,
                StandardCharsets.UTF_8
        )) {

            if (linha.isBlank()) {
                continue;
            }

            Registo registo;

            try {
                registo = Registo.fromLine(linha);

            } catch (RuntimeException e) {
                throw new IOException(
                        "Registo invalido no registos.log.",
                        e
                );
            }

            if (registo.lsn() != lsnEsperado) {
                throw new IOException(
                        "Sequencia LSN invalida. Esperado="
                                + lsnEsperado
                                + ", encontrado=" + registo.lsn()
                );
            }

            resultado.add(registo);
            lsnEsperado++;
        }

        return List.copyOf(resultado);
    }

}
