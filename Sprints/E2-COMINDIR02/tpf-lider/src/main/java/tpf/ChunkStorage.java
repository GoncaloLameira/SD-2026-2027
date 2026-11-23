
package tpf;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.GeneralSecurityException;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;

public final class ChunkStorage implements
        ChunkManager.Destino, AutoCloseable {

    private static final Path EVENTOS =
            Path.of("eventos-lider.txt");

    private final SealedStore store;
    private final byte[] masterKey;

    private boolean fechado = false;

    public ChunkStorage() throws IOException {

        // Carrega a chave mestra existente.
        this.masterKey = MasterKey.loadOrCreate();

        try {
            this.store = new SealedStore();

        } catch (IOException e) {
            Arrays.fill(masterKey, (byte) 0);
            throw e;
        }
    }

    @Override
    public synchronized void guardar(byte[] dados)
            throws IOException {

        if (fechado) {
            throw new IOException(
                    "ChunkStorage ja foi fechado."
            );
        }

        if (dados == null || dados.length == 0) {
            throw new IOException(
                    "Nao e possivel guardar um chunk vazio."
            );
        }

        // 1. Reconstroi os registos para obter
        // os metadados do chunk.
        List<Registo> registos = Chunk.ler(dados);

        if (registos.isEmpty()) {
            throw new IOException(
                    "Chunk sem registos."
            );
        }

        long lsnInicial = registos.get(0).lsn();

        long lsnFinal = registos.get(registos.size() - 1).lsn();

        int quantidade = registos.size();

        // 2. Valida a sequencia dos LSN.
        for (int i = 1; i < registos.size(); i++) {

            long anterior = registos.get(i - 1).lsn();
            long atual = registos.get(i).lsn();

            if (atual != anterior + 1) {
                throw new IOException(
                        "LSN nao consecutivos no chunk: "
                                + anterior + " -> " + atual
                );
            }
        }

        try {

            // 3. Cifra o chunk com AES-256-GCM.
            Sealed sealed = ChunkCrypto.seal(
                    dados,
                    masterKey
            );

            // 4. Guarda os ficheiros .bin e .hdr.
            store.guardar(sealed);

            System.out.println(
                    "CHUNK CIFRADO E GUARDADO | id="
                            + sealed.chunkId()
            );

            // 5. Regista o evento SELADO
            // depois de guardar os ficheiros.

            try {
                registarEventoSelado(
                        sealed.chunkId(),
                        lsnInicial,
                        lsnFinal,
                        quantidade,
                        dados.length,
                        sealed.ciphertext().length
                );

            } catch (IOException e) {
                // Os ficheiros .bin e .hdr ja foram guardados.
                // Nao devemos tentar guardar novamente o chunk.
                System.err.println(
                        "AVISO: Chunk guardado, mas falhou "
                                + "o evento SELADO | id="
                                + sealed.chunkId()
                                + " | erro=" + e.getMessage()
                );
            }


            System.out.println(
                    "EVENTO SELADO | id="
                            + sealed.chunkId()
                            + " | LSN=" + lsnInicial
                            + ".." + lsnFinal
                            + " | registos=" + quantidade
                            + " | bytes=" + dados.length
            );

        } catch (GeneralSecurityException e) {

            throw new IOException(
                    "Erro ao cifrar o chunk.",
                    e
            );
        }
    }

    // Grava o evento SELADO de forma duravel.
    private void registarEventoSelado(
            String chunkId,
            long lsnInicial,
            long lsnFinal,
            int quantidade,
            int tamanhoOriginal,
            int tamanhoCifrado
    ) throws IOException {

        String linha =
                Instant.now().toEpochMilli()
                        + ";SELADO"
                        + ";chunkId=" + chunkId
                        + ";lsnInicial=" + lsnInicial
                        + ";lsnFinal=" + lsnFinal
                        + ";registos=" + quantidade
                        + ";bytes=" + tamanhoOriginal
                        + ";bytesCifrados=" + tamanhoCifrado
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

    @Override
    public synchronized void close() {

        if (!fechado) {

            fechado = true;

            // Limpa a copia da chave em memoria.
            Arrays.fill(masterKey, (byte) 0);
        }
    }
}
