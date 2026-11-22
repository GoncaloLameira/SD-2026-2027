
package tpf;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public final class Chunk {

    private final List<Registo> registos = new ArrayList<>();

    // Momento em que entrou o primeiro registo.
    private long primeiroRegistoEm = -1;

    private boolean selado = false;

    // 4 bytes para guardar a quantidade de registos.
    private int tamanhoDados = Integer.BYTES;

    public synchronized void adicionar(Registo registo) {

        if (selado) {
            throw new IllegalStateException(
                    "Nao e possivel alterar um chunk selado."
            );
        }

        if (registo == null) {
            throw new IllegalArgumentException(
                    "O registo nao pode ser nulo."
            );
        }

        byte[] dados = registo.toLine()
                .getBytes(StandardCharsets.UTF_8);

        // Tamanho do chunk depois de adicionar o novo registo.
        // Inclui 4 bytes para o tamanho de cada registo.
        long novoTamanho = (long) tamanhoDados
                + Integer.BYTES
                + dados.length;

        // Calcula o tamanho final com padding.
        long resto = novoTamanho % Wire.PAD_BYTES;

        long padding = resto == 0
                ? 0
                : Wire.PAD_BYTES - resto;

        long tamanhoFinal = novoTamanho + padding;

        // Impede ultrapassar os 65 536 bytes.
        if (tamanhoFinal > Wire.CHUNK_MAX_BYTES) {
            throw new IllegalStateException(
                    "O registo ultrapassa a capacidade do chunk."
            );
        }

        // O temporizador so comeca com o primeiro registo.
        if (registos.isEmpty()) {
            primeiroRegistoEm = System.currentTimeMillis();
        }

        registos.add(registo);
        tamanhoDados = (int) novoTamanho;
    }

    // Indica se atingiu a capacidade maxima do chunk.
    public synchronized boolean atingiuLimite() {

        return tamanhoDados >= Wire.CHUNK_MAX_BYTES;
    }

    // Indica se passaram os 5000 ms desde o primeiro registo.
    public synchronized boolean expirou() {

        return !registos.isEmpty()
                && primeiroRegistoEm >= 0
                && System.currentTimeMillis() - primeiroRegistoEm
                >= Wire.SELAGEM_MS;
    }

    public synchronized boolean vazio() {
        return registos.isEmpty();
    }

    public synchronized int quantidade() {
        return registos.size();
    }

    public synchronized int tamanhoDados() {
        return tamanhoDados;
    }

    // Devolve uma copia da lista, sem permitir alteracoes.
    public synchronized List<Registo> getRegistos() {

        return Collections.unmodifiableList(
                new ArrayList<>(registos)
        );
    }

    // Serializa os registos e acrescenta padding.
    // Formato:
    // [quantidade:4 bytes]
    // [tamanho:4 bytes][registo]
    // [tamanho:4 bytes][registo]
    // ...
    // [padding com zeros]
    public synchronized byte[] selar() throws IOException {

        if (selado) {
            throw new IllegalStateException(
                    "Chunk ja selado."
            );
        }

        if (registos.isEmpty()) {
            throw new IllegalStateException(
                    "Nao e possivel selar um chunk vazio."
            );
        }

        ByteArrayOutputStream buffer =
                new ByteArrayOutputStream();

        DataOutputStream out =
                new DataOutputStream(buffer);

        out.writeInt(registos.size());

        for (Registo registo : registos) {

            byte[] dados = registo.toLine()
                    .getBytes(StandardCharsets.UTF_8);

            out.writeInt(dados.length);
            out.write(dados);
        }

        out.flush();

        byte[] conteudo = buffer.toByteArray();

        int resto = conteudo.length % Wire.PAD_BYTES;

        int padding = resto == 0
                ? 0
                : Wire.PAD_BYTES - resto;

        int tamanhoFinal = conteudo.length + padding;

        // Segunda verificacao de seguranca.
        if (tamanhoFinal > Wire.CHUNK_MAX_BYTES) {
            throw new IOException(
                    "Chunk excede o tamanho maximo permitido."
            );
        }

        byte[] resultado = new byte[tamanhoFinal];

        System.arraycopy(
                conteudo,
                0,
                resultado,
                0,
                conteudo.length
        );

        // As posicoes restantes ficam preenchidas com zeros.
        selado = true;

        return resultado;
    }

    // Reconstrói os registos de um chunk serializado.
    public static List<Registo> ler(byte[] dados)
            throws IOException {

        if (dados == null
                || dados.length == 0
                || dados.length > Wire.CHUNK_MAX_BYTES
                || dados.length % Wire.PAD_BYTES != 0) {

            throw new IOException(
                    "Tamanho de chunk invalido."
            );
        }

        DataInputStream in = new DataInputStream(
                new ByteArrayInputStream(dados)
        );

        int quantidade = in.readInt();

        if (quantidade <= 0) {
            throw new IOException(
                    "Quantidade de registos invalida."
            );
        }

        List<Registo> resultado = new ArrayList<>();

        for (int i = 0; i < quantidade; i++) {

            if (in.available() < Integer.BYTES) {
                throw new IOException(
                        "Chunk incompleto: falta tamanho do registo."
                );
            }

            int tamanho = in.readInt();

            if (tamanho <= 0 || tamanho > in.available()) {
                throw new IOException(
                        "Tamanho de registo invalido."
                );
            }

            byte[] texto = new byte[tamanho];
            in.readFully(texto);

            try {
                resultado.add(
                        Registo.fromLine(
                                new String(
                                        texto,
                                        StandardCharsets.UTF_8
                                )
                        )
                );

            } catch (RuntimeException e) {
                throw new IOException(
                        "Registo invalido no chunk.",
                        e
                );
            }
        }

        // Verifica se o padding contem apenas zeros.
        while (in.available() > 0) {

            if (in.readByte() != 0) {
                throw new IOException(
                        "Padding invalido."
                );
            }
        }

        return resultado;
    }
}
