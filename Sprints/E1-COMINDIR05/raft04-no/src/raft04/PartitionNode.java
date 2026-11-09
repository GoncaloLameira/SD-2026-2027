
package raft04;

import java.io.IOException;
import java.nio.file.Path;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class PartitionNode {

    private final int partition;
    private final int nodeId;
    private final int groupSize;
    private final String api;

    private RaftState raftState;
    private RecordFile recordFile;
    private IpfsPubSub pubSub;

    private NodeRecovery recovery;
    private NodeElection election;
    private NodeMessaging messaging;
    private NodeLogger nodeLogger;

    private final LamportClock lamportClock = new LamportClock();

    private VectorClock vectorClock =
            new VectorClock(Wire.PARTITIONS);

    private final Partitioner partitioner =
            new Partitioner(Wire.PARTITIONS);

    private final List<SensorRecord> pendingRecords =
            new ArrayList<>();

    private volatile boolean running = true;

    private long heartbeatCounter = 0;

    private String address;

    private ScheduledExecutorService leaderScheduler;
    private ScheduledFuture<?> leaderTask;

    public PartitionNode(
            int partition,
            int nodeId,
            int groupSize,
            String api
    ) throws IOException {

        if (partition < 1 || partition > Wire.PARTITIONS) {
            throw new IllegalArgumentException("Partição inválida.");
        }

        if (groupSize < 1 || nodeId < 1 || nodeId > groupSize) {
            throw new IllegalArgumentException("Nó ou grupo inválido.");
        }

        this.partition = partition;
        this.nodeId = nodeId;
        this.groupSize = groupSize;
        this.api = api;

        initialize();
    }

    private void initialize() throws IOException {

        nodeLogger = new NodeLogger(partition, nodeId);

        recordFile = new RecordFile(
                "no-p" + partition + "-n" + nodeId + ".txt"
        );

        address = InetAddress.getLocalHost().getHostAddress();

        Path stateFile = Path.of(
                "estado-p" + partition + "-n" + nodeId + ".txt"
        );

        raftState = new RaftState(
                partition,
                nodeId,
                groupSize,
                Wire.ELEICAO_MIN_MS,
                Wire.ELEICAO_MAX_MS,
                stateFile,
                this::logRaftStateEvent
        );

        SensorRecord last = recordFile.lastRecord(partition);

        if (last != null) {
            lamportClock.restore(last.getLamport());
            vectorClock.merge(last.getVectorClock());
        }

        pubSub = new IpfsPubSub(api);

        messaging = new NodeMessaging(
                partition,
                pubSub,
                this::isIsolated,
                message -> logEvent("IPFS", message)
        );

        recovery = new NodeRecovery(
                partition,
                nodeId,
                recordFile,
                raftState,
                this::isIsolated,
                this::logEvent,
                this::updateClocks,
                this::resetClocks,
                this::restoreClocks
        );

        election = new NodeElection(
                partition,
                nodeId,
                recordFile,
                raftState,
                this::isIsolated,
                messaging::publish,
                this::logEvent,
                this::startLeaderSender,
                this::stopLeaderSender
        );

        System.out.println(
                "Nó iniciado | partição " + partition
                        + " | nó " + nodeId
                        + " | endereço " + address
                        + " | porta TCP " + Wire.nodePort(partition, nodeId)
        );

        if (isIsolated()) {
            System.out.println("AVISO: este nó está isolado.");
        }
    }
    private boolean isIsolated() {
        boolean isolated = Wire.isolated(partition, nodeId);
        if (isolated) {
            System.out.println("ISOLAMENTO ATIVO | p" + partition + " n" + nodeId);
        }
        return isolated;
    }

    // ---------------------------------------------------------
    // RELÓGIOS
    // ---------------------------------------------------------

    private void updateClocks(SensorRecord record) {
        lamportClock.update(record.getLamport());
        vectorClock.merge(record.getVectorClock());
    }

    private void resetClocks() {
        lamportClock.restore(0);
        vectorClock = new VectorClock(Wire.PARTITIONS);
        pendingRecords.clear();
    }

    private void restoreClocks(SensorRecord record) {
        updateClocks(record);
    }

    // ---------------------------------------------------------
    // SUBSCRIÇÕES
    // ---------------------------------------------------------

    private void startSubscriptions() {
        messaging.startSubscriptions(
                this::handleMessage,
                this::handleBatch,
                this::handleElectionMessage
        );
    }

    // ---------------------------------------------------------
    // RECEÇÃO DE MENSAGENS
    // ---------------------------------------------------------

    private void handleMessage(String message) {

        if (message == null || message.isBlank()) {
            return;
        }

        try {
            if (!"IN".equals(Wire.type(message))) {
                return;
            }

            if (!isIsolated()) {
                handleInput(message);
            }

        } catch (IOException | RuntimeException e) {
            logEvent("ERRO_MENSAGEM", e.getMessage());
        }
    }

    private synchronized void handleElectionMessage(String message)
            throws IOException {

        if (message == null || message.isBlank() || isIsolated()) {
            return;
        }

        switch (Wire.type(message)) {

            case "PEDIDO_VOTO":
                election.handleVoteRequest(message);
                break;

            case "VOTO":
                election.handleVoteReply(message);
                break;

            default:
                logEvent("MENSAGEM_ELEICAO_DESCONHECIDA", message);
                break;
        }
    }

    // ---------------------------------------------------------
    // ENTRADA DOS PRODUTORES
    // ---------------------------------------------------------

    private synchronized void handleInput(String message)
            throws IOException {

        String[] fields = message.split(";", -1);

        if (fields.length < 3) {
            return;
        }

        String sensorId = fields[1].trim();

        if (sensorId.isEmpty()) {
            return;
        }

        double temperature;

        try {
            temperature = Double.parseDouble(fields[2].trim());
        } catch (NumberFormatException e) {
            return;
        }

        if (!Double.isFinite(temperature)) {
            return;
        }

        if (partitioner.of(sensorId) != partition - 1) {
            return;
        }

        long term = raftState.termIfLeader();

        if (term < 0) {
            logEvent(
                    "IGNORADO",
                    "IN;nó=" + nodeId + ";motivo=nao_lider"
            );
            return;
        }

        long sequence = recordFile.lastSeq(partition) + 1;
        int lamport = lamportClock.tick();

        vectorClock.increment(partition - 1);

        SensorRecord record = new SensorRecord(
                partition,
                sequence,
                term,
                sensorId,
                temperature,
                System.currentTimeMillis(),
                lamport,
                vectorClock.getClock()
        );

        recordFile.append(record);
        pendingRecords.add(record);

        logEvent(
                "IN",
                "nó=" + nodeId
                        + ";seq=" + sequence
                        + ";termo=" + term
        );
    }

    // ---------------------------------------------------------
    // ELEIÇÕES
    // ---------------------------------------------------------

    private void startElection(long term) throws IOException {
        election.startElection(term);
    }

    // ---------------------------------------------------------
    // HEARTBEATS DO LÍDER
    // ---------------------------------------------------------

    private synchronized void startLeaderSender() {

        if (isIsolated() || raftState.termIfLeader() < 0) {
            return;
        }

        if (leaderTask != null
                && !leaderTask.isDone()
                && !leaderTask.isCancelled()) {
            return;
        }

        stopLeaderSender();

        pendingRecords.clear();
        heartbeatCounter = 0;

        leaderScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread thread = new Thread(
                    r,
                    "leader-sender-p" + partition + "-n" + nodeId
            );
            thread.setDaemon(true);
            return thread;
        });

        leaderTask = leaderScheduler.scheduleAtFixedRate(() -> {

            long currentTerm = raftState.termIfLeader();

            if (isIsolated() || currentTerm < 0) {
                stopLeaderSender();
                return;
            }

            try {
                sendHeartbeat(currentTerm);
            } catch (IOException e) {
                logEvent(
                        "ERRO_ENVIO_LOTE",
                        "termo=" + currentTerm + ";erro=" + e.getMessage()
                );
            }

        }, 0, Wire.PERIODO_HB_MS, TimeUnit.MILLISECONDS);
    }

    private synchronized void stopLeaderSender() {

        if (leaderTask != null) {
            leaderTask.cancel(false);
            leaderTask = null;
        }

        if (leaderScheduler != null) {
            leaderScheduler.shutdownNow();
            leaderScheduler = null;
        }
    }

    private void sendHeartbeat(long term) throws IOException {

        List<SensorRecord> recordsToSend;
        Batch batch;
        long currentHeartbeat;

        synchronized (this) {

            long currentTerm = raftState.termIfLeader();

            if (isIsolated() || currentTerm < 0 || currentTerm != term) {
                return;
            }

            heartbeatCounter++;
            currentHeartbeat = heartbeatCounter;

            SensorRecord last = recordFile.lastRecord(partition);

            long lastSeq = last == null ? 0 : last.getSequence();
            long lastTerm = last == null ? 0 : last.getTerm();

            recordsToSend = new ArrayList<>(pendingRecords);
            pendingRecords.clear();

            batch = new Batch(
                    term,
                    nodeId,
                    address,
                    Wire.nodePort(partition, nodeId),
                    currentHeartbeat,
                    lastSeq,
                    lastTerm,
                    recordsToSend
            );
        }

        try {
            publish(Wire.topicData(partition), batch.encode());

        } catch (IOException e) {

            synchronized (this) {
                if (raftState.termIfLeader() == term) {
                    pendingRecords.addAll(0, recordsToSend);
                }
            }

            logEvent(
                    "ERRO_ENVIO_LOTE",
                    "termo=" + term
                            + ";hb=" + currentHeartbeat
                            + ";erro=" + e.getMessage()
            );

            throw e;
        }

        saveHeartbeat(
                term,
                currentHeartbeat,
                batch.getLastSeq(),
                batch.getLastTerm(),
                recordsToSend.size(),
                batch.encodedBytes()
        );

        logEvent(
                "LOTE_PUBLICADO",
                "nó=" + nodeId
                        + ";termo=" + term
                        + ";hb=" + currentHeartbeat
                        + ";ultimoSeq=" + batch.getLastSeq()
                        + ";registos=" + recordsToSend.size()
                        + ";bytes=" + batch.encodedBytes()
        );
    }

    // ---------------------------------------------------------
    // RECEÇÃO DE LOTES E RECUPERAÇÃO
    // ---------------------------------------------------------

    private synchronized void handleBatch(String message)
            throws IOException {

        if (isIsolated()) {
            return;
        }

        Batch batch = Batch.decode(message, partition);

        if (batch.getLeader() == nodeId) {
            return;
        }

        boolean accepted = raftState.onBatch(
                batch.getTerm(),
                batch.getLeader()
        );

        if (!accepted) {
            logEvent(
                    "IGNORADO",
                    "LOTE;lider=" + batch.getLeader()
                            + ";termo=" + batch.getTerm()
            );
            return;
        }

        if (raftState.termIfLeader() < 0) {
            stopLeaderSender();
        }

        String host = batch.getAddress();
        int port = batch.getPort();

        if (host == null || host.isBlank()
                || port < 1 || port > 65535) {
            logEvent("LOTE_INVALIDO", "endereco_ou_porto_invalido");
            return;
        }

        long localLastSeq = recordFile.lastSeq(partition);

        boolean divergence = localLastSeq > batch.getLastSeq();

        for (SensorRecord received : batch.getRecords()) {

            if (divergence) {
                break;
            }

            if (received.getSequence() > localLastSeq) {
                continue;
            }

            SensorRecord local = recordFile.range(
                    partition,
                    received.getSequence(),
                    received.getSequence()
            ).stream().findFirst().orElse(null);

            if (local != null
                    && (local.getTerm() != received.getTerm()
                    || !local.toLine().equals(received.toLine()))) {

                divergence = true;
            }
        }
        if (!divergence && localLastSeq > 0) {

            SensorRecord localLast = recordFile.lastRecord(partition);

            if (localLast != null
                    && localLast.getTerm() != batch.getLastTerm()) {
                divergence = true;
            }
        }

        if (divergence) {

            boolean corrected = recovery.resolveDivergence(
                    host,
                    port,
                    batch.getLastSeq(),
                    batch.getLeader(),
                    batch.getTerm()
            );

            if (!corrected) {
                logEvent(
                        "RECUPERACAO_DIVERGENCIA_FALHOU",
                        "lider=" + batch.getLeader()
                );
                return;
            }

        } else {

            int received = 0;

            for (SensorRecord record : batch.getRecords()) {

                long sequence = record.getSequence();
                long lastSeq = recordFile.lastSeq(partition);

                if (sequence <= lastSeq) {
                    continue;
                }

                if (sequence > lastSeq + 1) {

                    if (!recovery.recoverRecords(
                            host, port, lastSeq + 1, sequence - 1)) {
                        return;
                    }
                }

                if (record.getSequence()
                        == recordFile.lastSeq(partition) + 1) {

                    recordFile.append(record);
                    updateClocks(record);
                    received++;
                }
            }

            localLastSeq = recordFile.lastSeq(partition);

            if (localLastSeq < batch.getLastSeq()) {
                if (!recovery.recoverRecords(
                        host, port, localLastSeq + 1, batch.getLastSeq())) {
                    return;
                }
            }

            System.out.println(
                    "Lote processado | novos registos: " + received
            );
        }

        SensorRecord finalRecord = recordFile.lastRecord(partition);

        long finalSeq = finalRecord == null ? 0 : finalRecord.getSequence();
        long finalTerm = finalRecord == null ? 0 : finalRecord.getTerm();

        if (finalSeq != batch.getLastSeq()
                || finalTerm != batch.getLastTerm()) {
            logEvent(
                    "DIVERGENCIA_FINAL",
                    "seqLocal=" + finalSeq
                            + ";seqLider=" + batch.getLastSeq()
                            + ";termoLocal=" + finalTerm
                            + ";termoLider=" + batch.getLastTerm()
            );
        }

        saveHeartbeat(
                batch.getTerm(),
                batch.getHb(),
                batch.getLastSeq(),
                batch.getLastTerm(),
                batch.getRecords().size(),
                batch.encodedBytes()
        );

        logEvent(
                "RECEBIDO",
                "lider=" + batch.getLeader()
                        + ";termo=" + batch.getTerm()
                        + ";hb=" + batch.getHb()
                        + ";ultimoSeq=" + batch.getLastSeq()
        );
    }

    // ---------------------------------------------------------
    // SERVIDOR TCP
    // ---------------------------------------------------------

    private void startTcpServer() {

        TcpRecoveryServer server = new TcpRecoveryServer(
                partition,
                nodeId,
                recordFile,
                raftState,
                this::isIsolated,
                this::logEvent
        );

        server.start();
    }

    // ---------------------------------------------------------
    // MONITORIZAÇÃO DAS ELEIÇÕES
    // ---------------------------------------------------------

    private void startElectionMonitor() {

        Thread thread = new Thread(() -> {

            while (running) {

                try {
                    Thread.sleep(Wire.VERIFICACAO_MS);

                    if (isIsolated()) {
                        continue;
                    }

                    long term = raftState.tick();

                    if (term >= 0) {
                        startElection(term);
                    }

                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;

                } catch (IOException e) {
                    logEvent("ERRO_ELEICAO", e.getMessage());
                }
            }

        }, "election-p" + partition + "-n" + nodeId);

        thread.setDaemon(true);
        thread.start();
    }

    // ---------------------------------------------------------
    // PUBLICAÇÃO
    // ---------------------------------------------------------

    private void publish(String topic, String message)
            throws IOException {
        messaging.publish(topic, message);
    }

    // ---------------------------------------------------------
    // LOGS
    // ---------------------------------------------------------

    private void logEvent(String event, String detail) {
        nodeLogger.logEvent(event, detail);
    }

    private void logRaftStateEvent(String message) {

        if (message == null || message.isBlank()) {
            return;
        }

        String[] fields = message.split(";", 3);

        if (fields.length < 2) {
            logEvent("ESTADO", message);
            return;
        }

        String event = fields[0].trim();

        String detail = fields.length == 3
                ? fields[2].trim()
                : fields[1].trim();

        logEvent(event, detail);
    }

    private void saveHeartbeat(
            long term,
            long hb,
            long lastSeq,
            long lastTerm,
            int numberOfRecords,
            int bytes
    ) throws IOException {

        nodeLogger.saveHeartbeat(
                term, hb, lastSeq, lastTerm, numberOfRecords, bytes
        );
    }

    // ---------------------------------------------------------
    // ARRANQUE
    // ---------------------------------------------------------

    public void start() {

        startTcpServer();
        startSubscriptions();
        startElectionMonitor();

        System.out.println(
                "Nó ativo | partição " + partition
                        + " | identificador " + nodeId
                        + " | grupo " + groupSize
        );
    }

    // ---------------------------------------------------------
    // MAIN
    // ---------------------------------------------------------

    public static void main(String[] args) {

        if (args.length < 2 || args.length > 3) {
            System.out.println(
                    "Utilização: PartitionNode <partição> <nó> [N]"
            );
            return;
        }

        try {
            int partition = Integer.parseInt(args[0]);
            int nodeId = Integer.parseInt(args[1]);

            int groupSize = args.length == 3
                    ? Integer.parseInt(args[2])
                    : Wire.GRUPO;

            String api = System.getProperty(
                    "raft04.api",
                    "http://127.0.0.1:5001/api/v0"
            );

            PartitionNode node = new PartitionNode(
                    partition, nodeId, groupSize, api
            );

            node.start();

            try {
                Thread.currentThread().join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }

        } catch (NumberFormatException e) {
            System.err.println("Os argumentos devem ser números inteiros.");

        } catch (Exception e) {
            System.err.println(
                    "Não foi possível iniciar o nó: " + e.getMessage()
            );
            e.printStackTrace();
        }
    }
}
