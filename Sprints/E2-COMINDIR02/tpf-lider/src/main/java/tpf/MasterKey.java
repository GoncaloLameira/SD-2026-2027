
package tpf;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.PosixFilePermission;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Set;

public final class MasterKey {

    private static final int KEY_BYTES = 32;
    private static final Path KEY_PATH = Path.of("master.key");

    private MasterKey() {
    }

    public static byte[] loadOrCreate() throws IOException {
        if (Files.exists(KEY_PATH)) {
            return readExistingKey();
        }

        byte[] key = new byte[KEY_BYTES];
        new SecureRandom().nextBytes(key);

        try {
            // CREATE_NEW impede substituir uma chave existente.
            try (FileChannel channel = FileChannel.open(
                    KEY_PATH,
                    StandardOpenOption.CREATE_NEW,
                    StandardOpenOption.WRITE)) {

                ByteBuffer buffer = ByteBuffer.wrap(key);
                while (buffer.hasRemaining()) {
                    channel.write(buffer);
                }

                channel.force(true);
            }

            restrictPermissions();
            return key.clone();

        } catch (java.nio.file.FileAlreadyExistsException e) {
            // Outro processo criou a chave entretanto.
            return readExistingKey();
        } finally {
            Arrays.fill(key, (byte) 0);
        }
    }

    private static byte[] readExistingKey() throws IOException {
        if (!Files.isRegularFile(KEY_PATH)) {
            throw new IOException(
                    "master.key existe mas não é um ficheiro regular."
            );
        }

        byte[] key = Files.readAllBytes(KEY_PATH);

        if (key.length != KEY_BYTES) {
            throw new IOException(
                    "master.key inválida: esperados 32 bytes, encontrados "
                            + key.length
            );
        }

        restrictPermissions();
        return key;
    }

    private static void restrictPermissions() throws IOException {
        // Em Linux/macOS: leitura e escrita apenas para o dono.
        if (Files.getFileStore(KEY_PATH)
                .supportsFileAttributeView("posix")) {

            Files.setPosixFilePermissions(
                    KEY_PATH,
                    Set.of(
                            PosixFilePermission.OWNER_READ,
                            PosixFilePermission.OWNER_WRITE
                    )
            );
        }
        // Em Windows, as permissões são geridas por ACLs.
    }
}
