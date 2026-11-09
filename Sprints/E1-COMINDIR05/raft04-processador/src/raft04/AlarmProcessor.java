
package raft04;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class AlarmProcessor {

    private static final String DEFAULT_API =
            "http://127.0.0.1:5001/api/v0";
    private static final double LIMIAR_ALARME = 28.0;

    private final IpfsPubSub ipfs;
    private final Partitioner partitioner;
    private final long[] vetor;
    private final Set<String>[] recebidos;
    private final HeartbeatMonitor[] monitores;
    private final long[] maiorTermo;

    private final Path ficheiroMonitor =
            Paths.get("monitor-processador.txt");
    private final Object logLock = new Object();

    private long lamport = 0;

    @SuppressWarnings({"unchecked", "rawtypes"})
    public AlarmProcessor(int n, String api) {
        if (n != Wire.PARTITIONS) {
            throw new IllegalArgumentException(
                    "O numero de particoes deve ser " + Wire.PARTITIONS);
        }

        ipfs = new IpfsPubSub(api);
        partitioner = new Partitioner(n);
        vetor = new long[n];
        maiorTermo = new long[n];

        recebidos = (Set<String>[]) new Set[n];
        monitores = new HeartbeatMonitor[n];

        for (int i = 0; i < n; i++) {
            recebidos[i] = new HashSet<>();
            maiorTermo[i] = 0;

            monitores[i] = new HeartbeatMonitor(
                    i, Wire.PERIODO_HB_MS, Wire.FATOR_TIMEOUT,
                    this::evento);
        }
    }

    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("Uso: AlarmProcessor <N> [api]");
            return;
        }

        int n = Integer.parseInt(args[0]);
        String api = args.length > 1 ? args[1] : DEFAULT_API;

        AlarmProcessor processador = new AlarmProcessor(n, api);
        processador.iniciar();
    }

    private void iniciar() {
        for (int k = 0; k < partitioner.count(); k++) {
            final int particao = k + 1;

            Thread thread = new Thread(() -> {
                while (!Thread.currentThread().isInterrupted()) {
                    try {
                        String topico = Wire.topicData(particao);

                        System.out.println("A subscrever: " + topico);

                        ipfs.subscribe(topico,
                                mensagem -> processarLote(mensagem, particao));

                    } catch (Exception e) {
                        System.err.println("Erro na particao "
                                + particao + ": " + e.getMessage());

                        try {
                            Thread.sleep(2000);
                        } catch (InterruptedException ex) {
                            Thread.currentThread().interrupt();
                        }
                    }
                }
            }, "AlarmProcessor-p" + particao);

            thread.start();
        }

        ScheduledExecutorService verificador =
                Executors.newSingleThreadScheduledExecutor(r -> {
                    Thread t = new Thread(r, "AlarmProcessor-verificacao");
                    t.setDaemon(true);
                    return t;
                });

        verificador.scheduleAtFixedRate(() -> {
                    try {
                        for (HeartbeatMonitor monitor : monitores) {
                            monitor.check();
                        }
                    } catch (Throwable e) {
                        System.err.println("Erro na verificacao: " + e);
                    }
                }, Wire.VERIFICACAO_MS, Wire.VERIFICACAO_MS,
                TimeUnit.MILLISECONDS);

        System.out.println("Processador de alarmes pronto.");
    }

    private void evento(String texto) {
        String linha = System.currentTimeMillis() + ";" + texto;

        synchronized (logLock) {
            System.out.println("[monitor] " + linha);

            try {
                Files.writeString(
                        ficheiroMonitor,
                        linha + System.lineSeparator(),
                        StandardCharsets.UTF_8,
                        StandardOpenOption.CREATE,
                        StandardOpenOption.APPEND);
            } catch (IOException e) {
                System.err.println(
                        "Erro ao escrever o monitor: " + e.getMessage());
            }
        }
    }

    private void processarLote(String mensagem, int particao) {
        Batch lote;

        try {
            lote = Batch.decode(mensagem, particao);
        } catch (IllegalArgumentException e) {
            System.err.println("LOTE invalido em p" + particao
                    + ": " + e.getMessage());
            return;
        }

        int indice = particao - 1;
        long termoLote = lote.getInc();

        // Ignorar lotes de termos inferiores ao último termo conhecido.
        synchronized (this) {
            if (termoLote < maiorTermo[indice]) {
                evento("LOTE_IGNORADO_TERMO_INFERIOR;p" + particao
                        + ";termo=" + termoLote
                        + ";maiorTermo=" + maiorTermo[indice]);
                return;
            }

            if (termoLote > maiorTermo[indice]) {
                maiorTermo[indice] = termoLote;
            }
        }

        monitores[indice].onHeartbeat(
                termoLote, lote.getLeader(), lote.getHb());

        List<SensorRecord> registos = lote.getRecords();

        String intervalo = registos.isEmpty()
                ? "-"
                : registos.get(0).getSequence() + "-"
                + registos.get(registos.size() - 1).getSequence();

        evento("RECEBIDO;p" + particao
                + ";term=" + termoLote
                + ";lider=" + lote.getLeader()
                + ";hb=" + lote.getHb()
                + ";ultimoSeq=" + lote.getLastSeq()
                + ";ultimoTerm=" + lote.getLastTerm()
                + ";n=" + registos.size()
                + ";seq=" + intervalo);

        for (SensorRecord registo : registos) {
            processarRegisto(registo, particao);
        }
    }

    private void processarRegisto(
            SensorRecord registo, int particao) {

        try {
            long sequencia = registo.getSequence();
            long termoRegisto = registo.getTerm();
            String sensor = registo.getSensorId();
            double temperatura = registo.getTemperature();
            long lRecebido = registo.getLamport();
            int[] componentes = registo.getVectorClock();

            if (componentes.length != vetor.length
                    || sequencia < 1 || termoRegisto < 1) {
                System.out.println(
                        "Registo com vetor, sequencia ou termo invalido.");
                return;
            }

            long[] recebido = new long[vetor.length];

            for (int i = 0; i < vetor.length; i++) {
                recebido[i] = componentes[i];

                if (recebido[i] < 0) {
                    return;
                }
            }

            long lAlarme;
            long[] vAlarme;

            int indice = particao - 1;

            synchronized (this) {
                // Identidade do registo: termo próprio + sequência.
                String identificador = termoRegisto + ";" + sequencia;

                if (!recebidos[indice].add(identificador)) {
                    return;
                }

                lamport = Math.max(lamport, lRecebido);

                for (int i = 0; i < vetor.length; i++) {
                    vetor[i] = Math.max(vetor[i], recebido[i]);
                }

                System.out.println(
                        "Recebido: " + sensor
                                + " p" + particao + "#" + sequencia
                                + " termo=" + termoRegisto
                                + " temp=" + temperatura
                );

                if (sensor.startsWith("ALARM-")) {
                    return;
                }

                if (temperatura < LIMIAR_ALARME) {
                    return;
                }

                lAlarme = lamport;
                vAlarme = vetor.clone();
            }

            String sensorAlarme = "ALARM-" + sensor;

            // Partitioner devolve 0..2; os tópicos usam 1..3.
            int destino = partitioner.of(sensorAlarme) + 1;
            int indiceDestino = destino - 1;

            if (monitores[indiceDestino].state()
                    == HeartbeatMonitor.State.SUSPEITO) {
                System.out.println(
                        "AVISO: partição p" + destino
                                + " sem líder conhecido; alarme "
                                + sensorAlarme + " pode perder-se");
            }

            String mensagemAlarme =
                    "IN;" + sensorAlarme
                            + ";" + temperatura
                            + ";" + lAlarme
                            + ";" + vetorTexto(vAlarme);

            ipfs.publish(Wire.topicIn(destino), mensagemAlarme);

            System.out.println(
                    "ALARME: " + sensor
                            + " p" + particao + "#" + sequencia
                            + " -> p" + destino
                            + " L=" + lAlarme
                            + " V=" + vetorTexto(vAlarme)
            );

        } catch (Exception e) {
            System.err.println(
                    "Erro ao processar registo: " + e.getMessage());
        }
    }

    private static String vetorTexto(long[] v) {
        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < v.length; i++) {
            if (i > 0) {
                sb.append(',');
            }
            sb.append(v[i]);
        }

        return sb.toString();
    }
}
