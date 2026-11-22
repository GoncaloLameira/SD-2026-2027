
package tpf;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public final class ChunkManager implements AutoCloseable {

    @FunctionalInterface
    public interface Destino {
        void guardar(byte[] dados) throws IOException;
    }

    private final Destino destino;
    private final ScheduledExecutorService temporizador;

    private final Queue<byte[]> pendentes = new ArrayDeque<>();

    private Chunk atual = new Chunk();

    public ChunkManager(Destino destino) {
        if (destino == null) {
            throw new IllegalArgumentException(
                    "Destino do chunk nao pode ser nulo."
            );
        }

        this.destino = destino;

        temporizador =
                Executors.newSingleThreadScheduledExecutor();

        temporizador.scheduleWithFixedDelay(
                this::verificarTempo,
                200,
                200,
                TimeUnit.MILLISECONDS
        );
    }

    public synchronized void adicionar(Registo registo)
            throws IOException {

        // Tenta guardar chunks anteriores ainda pendentes.
        descarregarPendentes();

        if (atual.expirou()) {
            selarAtual();
        }

        try {
            atual.adicionar(registo);

        } catch (IllegalStateException e) {

            // Se o chunk ja tiver registos, sela-o e
            // tenta colocar o registo num novo chunk.
            if (atual.vazio()) {
                throw e;
            }

            selarAtual();
            atual.adicionar(registo);
        }

        if (atual.atingiuLimite()) {
            selarAtual();
        }
    }

    private synchronized void verificarTempo() {
        try {
            if (atual.expirou()) {
                selarAtual();
            }

            descarregarPendentes();

        } catch (IOException e) {
            System.err.println(
                    "Erro ao guardar chunk: " + e.getMessage()
            );
        }
    }

    private void selarAtual() throws IOException {
        if (atual.vazio()) {
            return;
        }

        int quantidade = atual.quantidade();

        byte[] dados = atual.selar();

        // Guarda os dados em memoria ate serem
        // entregues com sucesso ao destino.
        pendentes.add(dados);

        atual = new Chunk();

        System.out.println(
                "CHUNK SELADO | registos=" + quantidade
                        + " | bytes=" + dados.length
        );

        descarregarPendentes();
    }

    private void descarregarPendentes() throws IOException {
        while (!pendentes.isEmpty()) {
            byte[] dados = pendentes.peek();

            // Se falhar, nao remove o chunk da fila.
            destino.guardar(dados);

            pendentes.remove();
        }
    }

    public synchronized int quantidadeAtual() {
        return atual.quantidade();
    }

    public synchronized int chunksPendentes() {
        return pendentes.size();
    }

    @Override
    public synchronized void close() throws IOException {
        temporizador.shutdown();

        if (!atual.vazio()) {
            selarAtual();
        }

        descarregarPendentes();
    }
}

