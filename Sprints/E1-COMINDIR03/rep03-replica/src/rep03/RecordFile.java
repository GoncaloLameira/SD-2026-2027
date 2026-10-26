
package rep03;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.HashSet;
import java.util.Set;

public class RecordFile {

    private final Path path;

    public RecordFile(String fileName) {
        this.path = Paths.get(fileName);
    }

    // Guarda uma mensagem no ficheiro
// Guarda uma mensagem no ficheiro
    public synchronized void append(String mensagem)
            throws IOException {

        Files.writeString(
                path,
                mensagem + System.lineSeparator(),
                StandardCharsets.UTF_8,
                StandardOpenOption.CREATE,
                StandardOpenOption.APPEND
        );

        long instanteEscrita = System.currentTimeMillis();

        System.out.println(
                "ESCRITA_REPLICA | t=" + instanteEscrita
                        + " | " + mensagem
        );
    }
    // Extrai a particao e a sequencia de uma mensagem
    private long[] parseRecord(String mensagem) {

        int particao = -1;
        long sequencia = -1;

        if (!mensagem.trim().startsWith("REG |")) {
            return new long[]{-1, -1};
        }

        String[] partes = mensagem.split("\\|");

        for (String parte : partes) {

            String valor = parte.trim();

            if (valor.startsWith("p=")) {
                particao = Integer.parseInt(
                        valor.substring(2));
            }

            if (valor.startsWith("seq=")) {
                sequencia = Long.parseLong(
                        valor.substring(4));
            }
        }

        return new long[]{particao, sequencia};
    }

    // Devolve as sequencias guardadas numa particao
    private Set<Long> sequences(int partition)
            throws IOException {

        Set<Long> resultado = new HashSet<>();

        if (!Files.exists(path)) {
            return resultado;
        }

        for (String linha : Files.readAllLines(
                path, StandardCharsets.UTF_8)) {

            if (linha.isBlank()) {
                continue;
            }

            long[] dados = parseRecord(linha);

            if (dados[0] == partition && dados[1] >= 1) {
                resultado.add(dados[1]);
            }
        }

        return resultado;
    }

    // Verifica se uma sequencia ja esta guardada
    public synchronized boolean contains(
            int partition, long sequence)
            throws IOException {

        return sequences(partition).contains(sequence);
    }

    // Encontra a primeira sequencia em falta
    // Comeca na primeira sequencia existente no ficheiro
    public synchronized long firstMissing(
            int partition, long until)
            throws IOException {

        Set<Long> existentes = sequences(partition);

        // Se nao existem registos, nao existe
        // uma sequencia inicial conhecida
        if (existentes.isEmpty()) {
            return until + 1;
        }

        // Descobre a menor sequencia guardada
        long primeira = Long.MAX_VALUE;

        for (long seq : existentes) {
            primeira = Math.min(primeira, seq);
        }

        // Procura lacunas entre a primeira sequencia
        // guardada e o limite indicado
        for (long seq = primeira; seq <= until; seq++) {

            if (!existentes.contains(seq)) {
                return seq;
            }
        }

        return until + 1;
    }

    // Devolve a maior sequencia guardada na particao
    public synchronized long lastSeq(int partition)
            throws IOException {

        long ultima = 0;

        for (long seq : sequences(partition)) {
            ultima = Math.max(ultima, seq);
        }

        return ultima;
    }
}
