
package tpf;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.regex.Pattern;

public final class SealedStore {

    private static final Path PASTA = Path.of("selados");

    // Um chunkId deve ser um SHA-256 hexadecimal.
    private static final Pattern ID_VALIDO =
            Pattern.compile("[0-9a-f]{64}");

    public SealedStore() throws IOException {
        Files.createDirectories(PASTA);
    }

    // Guarda os dados cifrados e o respetivo cabecalho.
    public synchronized void guardar(Sealed sealed)
            throws IOException {

        validarId(sealed.chunkId());

        Path bin = PASTA.resolve(
                sealed.chunkId() + ".bin"
        );

        Path hdr = PASTA.resolve(
                sealed.chunkId() + ".hdr"
        );

        // Evita substituir chunks existentes.
        if (Files.exists(bin) || Files.exists(hdr)) {
            throw new IOException(
                    "Ja existem ficheiros para este chunkId."
            );
        }

        Path tempBin = Files.createTempFile(
                PASTA, "chunk-", ".bin.tmp"
        );

        Path tempHdr = Files.createTempFile(
                PASTA, "chunk-", ".hdr.tmp"
        );

        try {
            escrever(tempBin, sealed.ciphertext());
            escrever(tempHdr, sealed.headerBytes());

            // Publica primeiro os dados cifrados.
            mover(tempBin, bin);

            // O cabecalho e publicado por ultimo.
            // A sua presenca indica que ambos
            // os ficheiros devem estar completos.
            mover(tempHdr, hdr);

            System.out.println(
                    "CHUNK GUARDADO | id=" + sealed.chunkId()
                            + " | bytes=" + sealed.ciphertext().length
            );

        } finally {
            Files.deleteIfExists(tempBin);
            Files.deleteIfExists(tempHdr);
        }
    }

    // Le um chunk ja armazenado.
    public Sealed carregar(String chunkId) throws IOException {

        validarId(chunkId);

        Path bin = PASTA.resolve(chunkId + ".bin");
        Path hdr = PASTA.resolve(chunkId + ".hdr");

        if (!Files.isRegularFile(bin)
                || !Files.isRegularFile(hdr)) {
            throw new IOException(
                    "Chunk incompleto ou inexistente: " + chunkId
            );
        }

        byte[] ciphertext = Files.readAllBytes(bin);

        String header = Files.readString(
                hdr,
                StandardCharsets.UTF_8
        );

        try {
            return Sealed.fromFiles(
                    chunkId, header, ciphertext
            );

        } catch (RuntimeException e) {
            throw new IOException(
                    "Cabecalho ou dados invalidos.", e
            );
        }
    }

    private static void escrever(
            Path caminho,
            byte[] dados
    ) throws IOException {

        try (FileChannel channel = FileChannel.open(
                caminho,
                StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING
        )) {

            ByteBuffer buffer = ByteBuffer.wrap(dados);

            while (buffer.hasRemaining()) {
                channel.write(buffer);
            }

            channel.force(true);
        }
    }

    private static void mover(
            Path origem,
            Path destino
    ) throws IOException {

        try {
            Files.move(
                    origem,
                    destino,
                    StandardCopyOption.ATOMIC_MOVE
            );

        } catch (AtomicMoveNotSupportedException e) {
            Files.move(origem, destino);
        }
    }

    private static void validarId(String chunkId) {
        if (chunkId == null
                || !ID_VALIDO.matcher(chunkId).matches()) {
            throw new IllegalArgumentException(
                    "chunkId invalido."
            );
        }
    }
}

