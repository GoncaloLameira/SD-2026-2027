
package tpf;

import java.security.GeneralSecurityException;
import java.util.Arrays;
import java.util.List;

public class ChunkCryptoTest {

    public static void main(String[] args) throws Exception {

        System.out.println("=== TESTE DE CIFRA DO SPRINT 2 ===");

        byte[] masterKey = MasterKey.loadOrCreate();

        try {
            // 1. Criar tres registos de teste.
            Chunk chunk = new Chunk();

            chunk.adicionar(new Registo(
                    1, System.currentTimeMillis(),
                    "teste", "arquivo", "Primeira mensagem"
            ));

            chunk.adicionar(new Registo(
                    2, System.currentTimeMillis(),
                    "teste", "arquivo", "Segunda mensagem"
            ));

            chunk.adicionar(new Registo(
                    3, System.currentTimeMillis(),
                    "teste", "arquivo", "Terceira mensagem"
            ));

            List<Registo> originais = chunk.getRegistos();

            // 2. Selar o chunk com padding.
            byte[] dados = chunk.selar();

            System.out.println(
                    "Chunk serializado: " + dados.length + " bytes"
            );

            if (dados.length % Wire.PAD_BYTES != 0) {
                throw new AssertionError("Padding incorreto.");
            }

            // 3. Cifrar.
            Sealed sealed = ChunkCrypto.seal(dados, masterKey);

            System.out.println(
                    "Chunk cifrado: " + sealed.chunkId()
            );

            // 4. Guardar no disco.
            SealedStore store = new SealedStore();
            store.guardar(sealed);

            System.out.println("Ficheiros .bin e .hdr guardados.");

            // 5. Carregar e decifrar.
            Sealed carregado = store.carregar(sealed.chunkId());

            byte[] recuperados = ChunkCrypto.open(
                    carregado, masterKey
            );

            List<Registo> reconstruidos = Chunk.ler(recuperados);

            if (!originais.equals(reconstruidos)) {
                throw new AssertionError(
                        "Os registos recuperados sao diferentes."
                );
            }

            System.out.println(
                    "OK: " + reconstruidos.size()
                            + " registos recuperados corretamente."
            );

            // 6. Testar alteracao do ciphertext.
            byte[] adulterado = carregado.ciphertext();
            adulterado[0] ^= 1;

            Sealed modificado = new Sealed(
                    carregado.chunkId(),
                    carregado.keyVersion(),
                    carregado.salt(),
                    carregado.nonce(),
                    adulterado
            );

            try {
                ChunkCrypto.open(modificado, masterKey);

                throw new AssertionError(
                        "ERRO: ciphertext adulterado foi aceite."
                );

            } catch (GeneralSecurityException esperado) {
                System.out.println(
                        "OK: ciphertext adulterado rejeitado."
                );
            }

            // 7. Testar chave errada.
            byte[] chaveErrada = masterKey.clone();
            chaveErrada[0] ^= 1;

            try {
                try {
                    ChunkCrypto.open(carregado, chaveErrada);

                    throw new AssertionError(
                            "ERRO: chave errada foi aceite."
                    );

                } catch (GeneralSecurityException esperado) {
                    System.out.println(
                            "OK: chave errada rejeitada."
                    );
                }

                // 8. Testar salt adulterado.
                byte[] saltErrado = carregado.salt();
                saltErrado[0] ^= 1;

                Sealed saltModificado = new Sealed(
                        carregado.chunkId(),
                        carregado.keyVersion(),
                        saltErrado,
                        carregado.nonce(),
                        carregado.ciphertext()
                );

                try {
                    ChunkCrypto.open(saltModificado, masterKey);
                    throw new AssertionError(
                            "ERRO: salt adulterado foi aceite."
                    );
                } catch (GeneralSecurityException esperado) {
                    System.out.println(
                            "OK: salt adulterado rejeitado."
                    );
                }

                // 9. Testar nonce adulterado.
                byte[] nonceErrado = carregado.nonce();
                nonceErrado[0] ^= 1;

                Sealed nonceModificado = new Sealed(
                        carregado.chunkId(),
                        carregado.keyVersion(),
                        carregado.salt(),
                        nonceErrado,
                        carregado.ciphertext()
                );

                try {
                    ChunkCrypto.open(nonceModificado, masterKey);
                    throw new AssertionError(
                            "ERRO: nonce adulterado foi aceite."
                    );
                } catch (GeneralSecurityException esperado) {
                    System.out.println(
                            "OK: nonce adulterado rejeitado."
                    );
                }

                // 10. Testar versao da chave adulterada.
                Sealed versaoModificada = new Sealed(
                        carregado.chunkId(),
                        carregado.keyVersion() + 1,
                        carregado.salt(),
                        carregado.nonce(),
                        carregado.ciphertext()
                );

                try {
                    ChunkCrypto.open(versaoModificada, masterKey);
                    throw new AssertionError(
                            "ERRO: versao adulterada foi aceite."
                    );
                } catch (GeneralSecurityException esperado) {
                    System.out.println(
                            "OK: keyVersion adulterada rejeitada."
                    );
                }

            } finally {
                Arrays.fill(chaveErrada, (byte) 0);
            }

            System.out.println(
                    "=== TODOS OS TESTES PASSARAM ==="
            );

        } finally {
            Arrays.fill(masterKey, (byte) 0);
        }
    }
}
