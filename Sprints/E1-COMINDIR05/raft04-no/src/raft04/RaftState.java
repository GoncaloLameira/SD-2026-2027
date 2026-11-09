
package raft04;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;

public final class RaftState {

    public enum Role {
        SEGUIDOR, CANDIDATO, LIDER
    }

    public static final class Vote {
        public final long term;
        public final boolean granted;

        Vote(long term, boolean granted) {
            this.term = term;
            this.granted = granted;
        }
    }

    private final int partition;
    private final int self;
    private final int groupSize;
    private final long minNanos;
    private final long maxNanos;
    private final Path stateFile;
    private final Consumer<String> log;

    // DEFEITO 2: estado apenas em memória.
    // Após reiniciar, term e votedFor voltam aos valores iniciais.
    private long term = 0;
    private int votedFor = 0;

    // Estado volátil
    private Role role = Role.SEGUIDOR;
    private int leader = 0;
    private final Set<Integer> votes = new HashSet<>();
    private long deadline;

    public RaftState(
            int partition,
            int self,
            int groupSize,
            long minMs,
            long maxMs,
            Path stateFile,
            Consumer<String> log
    ) throws IOException {

        if (partition < 1 || partition > Wire.PARTITIONS) {
            throw new IllegalArgumentException(
                    "Particao invalida: " + partition
            );
        }

        if (groupSize < 1 || self < 1 || self > groupSize) {
            throw new IllegalArgumentException(
                    "Identificador do no invalido: " + self
            );
        }

        if (minMs <= 0 || maxMs < minMs) {
            throw new IllegalArgumentException(
                    "Intervalo de eleicao invalido"
            );
        }

        this.partition = partition;
        this.self = self;
        this.groupSize = groupSize;
        this.minNanos = minMs * 1_000_000L;
        this.maxNanos = maxMs * 1_000_000L;
        this.stateFile = stateFile;
        this.log = log;

        // DEFEITO 2:
        // Não carrega term nem votedFor do ficheiro de estado.
        resetTimer();
    }

    // Temporizador aleatório de eleição
    private void resetTimer() {
        long timeout = (maxNanos > minNanos)
                ? ThreadLocalRandom.current().nextLong(
                minNanos, maxNanos + 1
        )
                : minNanos;

        deadline = System.nanoTime() + timeout;
    }

    // DEFEITO 2: não persiste o estado.
    private void persist() {
        // Intencionalmente vazio para reproduzir o defeito.
    }

    // Adota um termo superior
    private void adoptTerm(long newTerm) {
        if (newTerm <= term) {
            return;
        }

        if (role == Role.LIDER) {
            log.accept(
                    "DEMISSAO;p" + partition + ";termo=" + term
            );
        }

        log.accept(
                "TERMO;p" + partition + ";" + term + "->" + newTerm
        );

        term = newTerm;
        votedFor = 0;
        role = Role.SEGUIDOR;
        leader = 0;

        votes.clear();

        persist();
    }

    // Verifica se chegou o momento de iniciar uma eleição
    public synchronized long tick() {
        if (role == Role.LIDER || System.nanoTime() < deadline) {
            return -1;
        }

        term++;
        votedFor = self;

        persist();

        role = Role.CANDIDATO;
        leader = 0;

        votes.clear();
        votes.add(self);

        resetTimer();

        log.accept(
                "CANDIDATO;p" + partition + ";termo=" + term
        );

        return term;
    }

    // Recebe um pedido de voto
    public synchronized Vote onVoteRequest(
            long cTerm,
            int candidate,
            long cLastSeq,
            long cLastTerm,
            long myLastSeq,
            long myLastTerm
    ) {

        if (candidate < 1 || candidate > groupSize || cTerm < 0) {
            return new Vote(term, false);
        }

        if (cTerm > term) {
            adoptTerm(cTerm);
        }

        boolean upToDate =
                cLastTerm > myLastTerm
                        || (cLastTerm == myLastTerm && cLastSeq >= myLastSeq);

        boolean granted =
                cTerm == term
                        && (votedFor == 0 || votedFor == candidate)
                        && upToDate;

        if (granted) {
            votedFor = candidate;
            role = Role.SEGUIDOR;
            leader = 0;

            votes.clear();

            persist();
            resetTimer();
        }

        log.accept(
                "VOTO;p" + partition
                        + ";termo=" + term
                        + ";candidato=n" + candidate
                        + ";" + (granted ? "concedido" : "recusado")
        );

        return new Vote(term, granted);
    }

    // Recebe uma resposta a um pedido de voto
    public synchronized boolean onVote(
            long vTerm,
            int voter,
            int candidate,
            boolean granted
    ) {

        if (voter < 1 || voter > groupSize
                || candidate < 1 || candidate > groupSize) {
            return false;
        }

        if (vTerm > term) {
            adoptTerm(vTerm);
            return false;
        }

        if (role != Role.CANDIDATO
                || vTerm != term
                || candidate != self
                || !granted) {
            return false;
        }

        // HashSet impede contar o mesmo voto mais do que uma vez
        votes.add(voter);

        // Maioria absoluta — restaurada corretamente para este ensaio
        if (votes.size() <= groupSize / 2) {
            return false;
        }

        role = Role.LIDER;
        leader = self;

        log.accept(
                "LIDER;p" + partition
                        + ";termo=" + term
                        + ";votos=" + votes
        );

        return true;
    }

    // Recebe um lote de outro nó
    public synchronized boolean onBatch(
            long bTerm,
            int bLeader
    ) {

        if (bLeader < 1 || bLeader > groupSize || bTerm < 0) {
            return false;
        }

        if (bTerm < term) {
            return false;
        }

        if (bTerm > term) {
            adoptTerm(bTerm);
        }

        if (role == Role.LIDER) {
            log.accept(
                    "ERRO;p" + partition
                            + ";dois lideres no termo " + term
            );
            return false;
        }

        role = Role.SEGUIDOR;

        if (leader != bLeader) {
            leader = bLeader;

            log.accept(
                    "NOVO_LIDER;p" + partition
                            + ";termo=" + term
                            + ";lider=n" + bLeader
            );
        }

        votes.clear();
        persist();
        resetTimer();

        return true;
    }

    public synchronized long termIfLeader() {
        return role == Role.LIDER ? term : -1;
    }

    public synchronized long term() {
        return term;
    }

    public synchronized Role role() {
        return role;
    }

    public synchronized int leader() {
        return leader;
    }
}
