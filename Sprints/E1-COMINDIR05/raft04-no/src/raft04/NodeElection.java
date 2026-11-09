package raft04;

import java.io.IOException;
import java.util.function.BiConsumer;
import java.util.function.BooleanSupplier;

public final class NodeElection {

    @FunctionalInterface
    public interface Publisher {
        void publish(String topic, String message) throws IOException;
    }

    private final int partition;
    private final int nodeId;

    private final RecordFile recordFile;
    private final RaftState raftState;

    private final BooleanSupplier isolated;
    private final Publisher publisher;
    private final BiConsumer<String, String> logger;

    private final Runnable startLeaderSender;
    private final Runnable stopLeaderSender;

    public NodeElection(
            int partition,
            int nodeId,
            RecordFile recordFile,
            RaftState raftState,
            BooleanSupplier isolated,
            Publisher publisher,
            BiConsumer<String, String> logger,
            Runnable startLeaderSender,
            Runnable stopLeaderSender
    ) {
        this.partition = partition;
        this.nodeId = nodeId;
        this.recordFile = recordFile;
        this.raftState = raftState;
        this.isolated = isolated;
        this.publisher = publisher;
        this.logger = logger;
        this.startLeaderSender = startLeaderSender;
        this.stopLeaderSender = stopLeaderSender;
    }

    // ---------------------------------------------------------
    // PEDIDOS DE VOTO
    // ---------------------------------------------------------

    public void handleVoteRequest(String message)
            throws IOException {

        if (isolated.getAsBoolean()) {
            return;
        }
  
        VoteRequest request = VoteRequest.decode(message);

        if (request.candidate == nodeId) {
            return;
        }

        SensorRecord last = recordFile.lastRecord(partition);

        long myLastSeq = last == null ? 0 : last.getSequence();
        long myLastTerm = last == null ? 0 : last.getTerm();

        RaftState.Vote vote = raftState.onVoteRequest(
                request.term,
                request.candidate,
                request.lastSeq,
                request.lastTerm,
                myLastSeq,
                myLastTerm
        );

        if (raftState.termIfLeader() < 0) {
            stopLeaderSender.run();
        }

        VoteReply reply = new VoteReply(
                vote.term,
                nodeId,
                request.candidate,
                vote.granted
        );

        publisher.publish(
                Wire.topicElection(partition),
                reply.encode()
        );

        System.out.println(
                "Pedido de voto recebido de n" + request.candidate
                        + ": "
                        + (vote.granted ? "concedido" : "recusado")
        );
    }

    // ---------------------------------------------------------
    // RESPOSTAS A PEDIDOS DE VOTO
    // ---------------------------------------------------------

    public void handleVoteReply(String message) {

        if (isolated.getAsBoolean()) {
            return;
        }

        VoteReply reply = VoteReply.decode(message);

        if (reply.candidate != nodeId || reply.voter == nodeId) {
            return;
        }

        boolean becameLeader = raftState.onVote(
                reply.term,
                reply.voter,
                reply.candidate,
                reply.granted
        );

        if (raftState.termIfLeader() < 0) {
            stopLeaderSender.run();
        }

        if (becameLeader) {

            System.out.println(
                    "O nó " + nodeId
                            + " foi eleito líder da partição "
                            + partition
            );

            startLeaderSender.run();
        }
    }

    // ---------------------------------------------------------
    // INÍCIO DE UMA ELEIÇÃO
    // ---------------------------------------------------------

    public void startElection(long term) throws IOException {

        if (isolated.getAsBoolean()) {
            return;
        }

        SensorRecord last = recordFile.lastRecord(partition);

        long lastSeq = last == null ? 0 : last.getSequence();
        long lastTerm = last == null ? 0 : last.getTerm();

        boolean becameLeader = raftState.onVote(
                term,
                nodeId,
                nodeId,
                true
        );

        if (raftState.term() != term
                || raftState.role() != RaftState.Role.CANDIDATO) {
            return;
        }

        System.out.println(
                "Eleição iniciada | partição " + partition
                        + " | candidato n" + nodeId
                        + " | termo " + term
        );

        if (becameLeader) {

            System.out.println(
                    "O nó " + nodeId
                            + " foi eleito líder da partição "
                            + partition
            );

            startLeaderSender.run();
            return;
        }

        VoteRequest request = new VoteRequest(
                term,
                nodeId,
                lastSeq,
                lastTerm
        );

        publisher.publish(
                Wire.topicElection(partition),
                request.encode()
        );
    }
}