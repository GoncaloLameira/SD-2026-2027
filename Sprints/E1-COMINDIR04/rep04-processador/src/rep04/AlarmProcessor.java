package rep04;

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

    private static final String DEFAULT_API = "http://127.0.0.1:5001/api/v0";
    private static final double LIMIAR_ALARME = 28.0;

    private final IpfsPubSub ipfs;
    private final Partitioner partitioner;
    private final long[] vetor;
    private final Set<Long>[] recebidos;

    // Um detetor de falhas por particao
    private final HeartbeatMonitor[] monitores;

    // Eventos do detetor: instante;evento;p<k>;detalhe
    private final Path ficheiroMonitor = Paths.get("monitor-processador.txt");
    private final Object logLock = new Object();

    private long lamport = 0;

    @SuppressWarnings({"unchecked", "rawtypes"})
    public AlarmProcessor(int n, String api) {
        ipfs = new IpfsPubSub(api);
        partitioner = new Partitioner(n);
        vetor = new long[n];

        recebidos = (Set<Long>[]) new Set[n];
        monitores = new HeartbeatMonitor[n];

        for (int i = 0; i < n; i++) {
            recebidos[i] = new HashSet<>();
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
            final int particao = k;

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
            }, "AlarmProcessor-p" + k);

            thread.start();
        }

        // Thread de verificacao: check() em todos os monitores
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
        }, Wire.VERIFICACAO_MS, Wire.VERIFICACAO_MS, TimeUnit.MILLISECONDS);

        System.out.println("Processador de alarmes pronto.");
    }

    // Escreve um evento na consola e em monitor-processador.txt
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

    // Chegou um LOTE da particao: renova o detetor e trata os registos
    private void processarLote(String mensagem, int particao) {
        Batch lote;

        try {
            lote = Batch.decode(mensagem, particao);
        } catch (IllegalArgumentException e) {
            System.err.println("LOTE invalido em p" + particao
                    + ": " + e.getMessage());
            return;
        }

        monitores[particao].onHeartbeat(lote.getInc(), lote.getHb());

        List<SensorRecord> registos = lote.getRecords();
        String intervalo = registos.isEmpty()
                ? "-"
                : registos.get(0).getSequence() + "-"
                + registos.get(registos.size() - 1).getSequence();

        evento("RECEBIDO;p" + particao
                + ";inc=" + lote.getInc()
                + ";hb=" + lote.getHb()
                + ";ultimoSeq=" + lote.getLastSeq()
                + ";n=" + registos.size()
                + ";seq=" + intervalo);

        // Os registos sao tratados mesmo que o detetor tenha ignorado o
        // heartbeat (por exemplo, inc antiga): os duplicados caem em
        // recebidos[particao].
        for (SensorRecord registo : registos) {
            processarRegisto(registo, particao);
        }
    }

    private void processarRegisto(SensorRecord registo, int particao) {
        try {
            long sequencia = registo.getSequence();
            String sensor = registo.getSensorId();
            double temperatura = registo.getTemperature();
            long lRecebido = registo.getLamport();
            int[] componentes = registo.getVectorClock();

            if (componentes.length != vetor.length || sequencia < 1) {
                System.out.println("Registo com vetor ou sequencia invalida.");
                return;
            }

            long[] recebido = new long[vetor.length];

            for (int i = 0; i < vetor.length; i++) {
                recebido[i] = componentes[i];
                if (recebido[i] < 0) return;
            }

            long lAlarme;
            long[] vAlarme;

            synchronized (this) {
                if (!recebidos[particao].add(sequencia)) {
                    return;
                }

                // Acumula o conhecimento de TODOS os registos.
                lamport = Math.max(lamport, lRecebido);

                for (int i = 0; i < vetor.length; i++) {
                    vetor[i] = Math.max(vetor[i], recebido[i]);
                }

                System.out.println(
                        "Recebido: " + sensor
                                + " p" + particao + "#" + sequencia
                                + " temp=" + temperatura
                );

                // Evita alarmes sobre alarmes.
                if (sensor.startsWith("ALARM-")) {
                    return;
                }

                if (temperatura < LIMIAR_ALARME) {
                    return;
                }

                // Fotografia do conhecimento causal neste instante.
                lAlarme = lamport;
                vAlarme = vetor.clone();
            }

            String sensorAlarme = "ALARM-" + sensor;
            int destino = partitioner.of(sensorAlarme);

            if (monitores[destino].state() == HeartbeatMonitor.State.SUSPEITO) {
                System.out.println(
                        "AVISO: líder p" + destino
                                + " suspeito; alarme " + sensorAlarme
                                + " pode perder-se");
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
                    "Erro ao processar registo: " + e.getMessage()
            );
        }
    }

    private static String vetorTexto(long[] v) {
        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < v.length; i++) {
            if (i > 0) sb.append(',');
            sb.append(v[i]);
        }

        return sb.toString();
    }
}
