
package tpf;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

public record Sealed(
        String chunkId,
        int keyVersion,
        byte[] salt,
        byte[] nonce,
        byte[] ciphertext
) {

    public Sealed {
        if (chunkId == null || chunkId.isBlank()) {
            throw new IllegalArgumentException(
                    "chunkId invalido."
            );
        }

        if (keyVersion <= 0) {
            throw new IllegalArgumentException(
                    "Versao da chave invalida."
            );
        }

        if (salt == null || salt.length != 32) {
            throw new IllegalArgumentException(
                    "O salt deve ter 32 bytes."
            );
        }

        if (nonce == null || nonce.length != 12) {
            throw new IllegalArgumentException(
                    "O nonce deve ter 12 bytes."
            );
        }

        if (ciphertext == null || ciphertext.length < 16) {
            throw new IllegalArgumentException(
                    "Texto cifrado invalido."
            );
        }

        // Copias defensivas para proteger os arrays.
        salt = salt.clone();
        nonce = nonce.clone();
        ciphertext = ciphertext.clone();
    }

    @Override
    public byte[] salt() {
        return salt.clone();
    }

    @Override
    public byte[] nonce() {
        return nonce.clone();
    }

    @Override
    public byte[] ciphertext() {
        return ciphertext.clone();
    }

    // Conteudo do ficheiro .hdr:
    // keyVersion;saltBase64;nonceBase64
    public String toHeader() {
        return keyVersion + ";"
                + Base64.getEncoder().encodeToString(salt)
                + ";"
                + Base64.getEncoder().encodeToString(nonce);
    }

    // Reconstroi um chunk cifrado a partir do .hdr e .bin.
    public static Sealed fromFiles(
            String chunkId,
            String header,
            byte[] ciphertext
    ) {
        String[] campos = header.trim().split(";", -1);

        if (campos.length != 3) {
            throw new IllegalArgumentException(
                    "Cabecalho do chunk invalido."
            );
        }

        return new Sealed(
                chunkId,
                Integer.parseInt(campos[0]),
                Base64.getDecoder().decode(campos[1]),
                Base64.getDecoder().decode(campos[2]),
                ciphertext
        );
    }

    public byte[] headerBytes() {
        return toHeader().getBytes(StandardCharsets.UTF_8);
    }
}
