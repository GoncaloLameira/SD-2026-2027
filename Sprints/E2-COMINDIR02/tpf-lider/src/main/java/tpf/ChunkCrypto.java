
package tpf;

import javax.crypto.Cipher;
import javax.crypto.Mac;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.HexFormat;

public final class ChunkCrypto {

    private static final int KEY_BYTES = 32;
    private static final int SALT_BYTES = 32;
    private static final int NONCE_BYTES = 12;
    private static final int TAG_BITS = 128;
    private static final int KEY_VERSION = 1;

    private static final byte[] INFO =
            "tpf-chunk-v1".getBytes(StandardCharsets.UTF_8);

    private static final byte[] AAD_PREFIX =
            "tpf-v1".getBytes(StandardCharsets.UTF_8);

    private static final SecureRandom RANDOM = new SecureRandom();

    private ChunkCrypto() {
    }

    // Cifra um chunk com AES-256-GCM.
    public static Sealed seal(
            byte[] plaintext,
            byte[] masterKey
    ) throws GeneralSecurityException {

        validarChave(masterKey);

        if (plaintext == null || plaintext.length == 0) {
            throw new IllegalArgumentException(
                    "O chunk nao pode estar vazio."
            );
        }

        if (plaintext.length % Wire.PAD_BYTES != 0
                || plaintext.length > Wire.CHUNK_MAX_BYTES) {
            throw new IllegalArgumentException(
                    "Tamanho do chunk invalido."
            );
        }

        byte[] salt = new byte[SALT_BYTES];
        byte[] nonce = new byte[NONCE_BYTES];

        RANDOM.nextBytes(salt);
        RANDOM.nextBytes(nonce);

        // Deriva uma chave diferente para cada chunk.
        byte[] dek = hkdf(masterKey, salt);

        try {
            Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");

            SecretKeySpec keySpec =
                    new SecretKeySpec(dek, "AES");

            GCMParameterSpec gcmSpec =
                    new GCMParameterSpec(TAG_BITS, nonce);

            cipher.init(
                    Cipher.ENCRYPT_MODE,
                    keySpec,
                    gcmSpec
            );

            // Autentica tambem os metadados do chunk.
            cipher.updateAAD(
                    criarAAD(KEY_VERSION, salt, nonce)
            );

            // Inclui a tag de autenticacao GCM.
            byte[] ciphertext = cipher.doFinal(plaintext);

            // Identificador baseado nos dados cifrados.
            String chunkId = sha256Hex(ciphertext);

            return new Sealed(
                    chunkId,
                    KEY_VERSION,
                    salt,
                    nonce,
                    ciphertext
            );

        } finally {
            // Apaga a copia temporaria da chave derivada.
            Arrays.fill(dek, (byte) 0);
        }
    }

    // Verifica e decifra um chunk.
    public static byte[] open(
            Sealed sealed,
            byte[] masterKey
    ) throws GeneralSecurityException {

        if (sealed == null) {
            throw new GeneralSecurityException(
                    "Chunk cifrado inexistente."
            );
        }

        validarChave(masterKey);

        byte[] ciphertext = sealed.ciphertext();

        // 1. Verifica primeiro o SHA-256 do ciphertext.
        String hashCalculado = sha256Hex(ciphertext);

        if (!MessageDigest.isEqual(
                hashCalculado.getBytes(StandardCharsets.US_ASCII),
                sealed.chunkId().getBytes(StandardCharsets.US_ASCII)
        )) {
            throw new GeneralSecurityException(
                    "Hash do chunk nao corresponde ao identificador."
            );
        }

        // 2. Verifica a versao da chave.
        if (sealed.keyVersion() != KEY_VERSION) {
            throw new GeneralSecurityException(
                    "Versao de chave nao suportada."
            );
        }

        byte[] salt = sealed.salt();
        byte[] nonce = sealed.nonce();

        // 3. Deriva a chave e verifica a autenticacao GCM.
        byte[] dek = hkdf(masterKey, salt);

        try {
            Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");

            cipher.init(
                    Cipher.DECRYPT_MODE,
                    new SecretKeySpec(dek, "AES"),
                    new GCMParameterSpec(TAG_BITS, nonce)
            );

            cipher.updateAAD(
                    criarAAD(sealed.keyVersion(), salt, nonce)
            );

            // Falha com excecao se a chave, os metadados
            // ou o ciphertext estiverem incorretos.
            return cipher.doFinal(ciphertext);

        } finally {
            Arrays.fill(dek, (byte) 0);
        }
    }

    // HKDF-SHA256: extracao e expansao de um unico bloco.
    private static byte[] hkdf(
            byte[] masterKey,
            byte[] salt
    ) throws GeneralSecurityException {

        // EXTRACT: PRK = HMAC(salt, masterKey)
        Mac mac = Mac.getInstance("HmacSHA256");

        mac.init(
                new SecretKeySpec(salt, "HmacSHA256")
        );

        byte[] prk = mac.doFinal(masterKey);

        try {
            // EXPAND: OKM = HMAC(PRK, info || 0x01)
            mac.init(
                    new SecretKeySpec(prk, "HmacSHA256")
            );

            mac.update(INFO);
            mac.update((byte) 0x01);

            byte[] resultado = mac.doFinal();

            return Arrays.copyOf(resultado, KEY_BYTES);

        } finally {
            Arrays.fill(prk, (byte) 0);
        }
    }

    // AAD = "tpf-v1" + keyVersion (4 bytes) + salt + nonce
    private static byte[] criarAAD(
            int keyVersion,
            byte[] salt,
            byte[] nonce
    ) {

        return ByteBuffer.allocate(
                        AAD_PREFIX.length
                                + Integer.BYTES
                                + salt.length
                                + nonce.length
                )
                .put(AAD_PREFIX)
                .putInt(keyVersion)
                .put(salt)
                .put(nonce)
                .array();
    }

    private static String sha256Hex(
            byte[] dados
    ) throws GeneralSecurityException {

        MessageDigest digest =
                MessageDigest.getInstance("SHA-256");

        return HexFormat.of().formatHex(
                digest.digest(dados)
        );
    }

    private static void validarChave(
            byte[] masterKey
    ) throws GeneralSecurityException {

        if (masterKey == null
                || masterKey.length != KEY_BYTES) {
            throw new GeneralSecurityException(
                    "A master.key deve ter exatamente 32 bytes."
            );
        }
    }
}
