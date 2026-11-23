
package tpf;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

public final class ChunkRecovery {

    private static final Path PASTA = Path.of("selados");

    private ChunkRecovery() {
    }

    public static Set<Long> identificarArquivados(
            RegistoLog registoLog
    ) throws IOException {

        // Le os registos originais do log persistente.
        List<Registo> originais = registoLog.lerTodos();

        Map<Long, Registo> porLsn = new HashMap<>();

        for (Registo registo : originais) {
            porLsn.put(registo.lsn(), registo);
        }

        Set<Long> arquivados = new HashSet<>();

        if (!Files.exists(PASTA)) {
            return arquivados;
        }

        byte[] masterKey = MasterKey.loadOrCreate();

        try {
            SealedStore store = new SealedStore();

            // Verifica se existem chunks incompletos:
            // ficheiros .bin sem o respetivo .hdr.
            try (Stream<Path> ficheirosBin = Files.list(PASTA)) {

                List<Path> bins = ficheirosBin
                        .filter(p -> p.getFileName()
                                .toString().endsWith(".bin"))
                        .toList();

                for (Path bin : bins) {

                    String nome = bin.getFileName().toString();

                    String chunkId = nome.substring(
                            0, nome.length() - 4
                    );

                    Path header = PASTA.resolve(
                            chunkId + ".hdr"
                    );

                    if (!Files.isRegularFile(header)) {

                        throw new IOException(
                                "CHUNK INCOMPLETO | "
                                        + "ficheiro HDR em falta | id="
                                        + chunkId
                        );
                    }
                }
            }


            try (Stream<Path> ficheiros = Files.list(PASTA)) {

                List<Path> headers = ficheiros
                        .filter(p -> p.getFileName()
                                .toString().endsWith(".hdr"))
                        .sorted()
                        .toList();

                for (Path header : headers) {

                    String nome = header.getFileName().toString();

                    String chunkId = nome.substring(
                            0, nome.length() - 4
                    );

                    try {
                        // Carrega o par .hdr + .bin.
                        Sealed sealed = store.carregar(chunkId);

                        // Verifica o hash e a autenticacao GCM.
                        byte[] dados = ChunkCrypto.open(
                                sealed,
                                masterKey
                        );

                        // Reconstroi os registos originais.
                        List<Registo> registos = Chunk.ler(dados);

                        boolean corresponde = true;

                        for (Registo registo : registos) {

                            Registo original =
                                    porLsn.get(registo.lsn());

                            if (!registo.equals(original)) {
                                corresponde = false;
                                break;
                            }
                        }

                        if (corresponde) {

                            for (Registo registo : registos) {
                                arquivados.add(registo.lsn());
                            }

                            System.out.println(
                                    "CHUNK RECUPERADO | registos="
                                            + registos.size()
                                            + " | id=" + chunkId
                            );

                        } else {
                            // Pode ser um chunk de teste.
                            // Nunca marca estes LSN como arquivados.
                            System.out.println(
                                    "CHUNK IGNORADO | dados nao "
                                            + "correspondem ao registos.log"
                                            + " | id=" + chunkId
                            );
                        }

                    } catch (GeneralSecurityException
                             | IllegalArgumentException e) {

                        throw new IOException(
                                "Falha de integridade no chunk: "
                                        + chunkId,
                                e
                        );
                    }
                }
            }

            System.out.println(
                    "RECUPERACAO | registos no log="
                            + originais.size()
                            + " | ja arquivados="
                            + arquivados.size()
            );

            return arquivados;

        } finally {
            Arrays.fill(masterKey, (byte) 0);
        }
    }
}
