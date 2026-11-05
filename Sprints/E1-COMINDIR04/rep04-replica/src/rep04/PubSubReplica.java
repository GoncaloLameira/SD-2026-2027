package rep04;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

// Uso: PubSubReplica [api]
// Propriedades de sistema:
//   -Drep04.replica=<r>           identificador da replica (por omissao 1)
//   -Drep04.particoes=0,1,2       particoes que a replica guarda (por omissao todas)
public class PubSubReplica {
    static final String DEFAULT_API = "http://127.0.0.1:5001/api/v0";
    static final int PARTITIONS = Wire.PARTITIONS;
    static final int TIMEOUT_MS = 5000;

    static final int REPLICA = Integer.getInteger("rep04.replica", 1);
    static final String PARTICOES =
            System.getProperty("rep04.particoes", "0,1,2");

    // Um detetor por particao guardada (null nas restantes)
    private static final HeartbeatMonitor[] monitores =
            new HeartbeatMonitor[PARTITIONS];

    // Eventos do detetor: instante;evento;p<k>;detalhe
    private static final Object logLock = new Object();
    private static final Path ficheiroMonitor =
            Paths.get("monitor-replica" + REPLICA + ".txt");

    public static void main(String[] args) {
        String api = args.length > 0 ? args[0] : DEFAULT_API;
        IpfsPubSub pubSub = new IpfsPubSub(api);
        RecordFile ficheiro =
                new RecordFile("rep04-replica" + REPLICA + "-registos.txt");

        List<Integer> particoes = new ArrayList<>();

        try {
            for (String s : PARTICOES.split(",")) {
                int p = Integer.parseInt(s.trim());

                if (p < 0 || p >= PARTITIONS) {
                    throw new IllegalArgumentException(
                            "Particao invalida: " + p);
                }
                if (!particoes.contains(p)) {
                    particoes.add(p);
                }
            }
        } catch (IllegalArgumentException e) {
            System.out.println("rep04.particoes invalido: " + e.getMessage());
            return;
        }

        for (int p : particoes) {
            monitores[p] = new HeartbeatMonitor(
                    p, Wire.PERIODO_HB_MS, Wire.FATOR_TIMEOUT,
                    PubSubReplica::evento);
        }

        System.out.println("Replica " + REPLICA + " pronta. A subscrever "
                + particoes.size() + " particoes: " + particoes);

        for (int p : particoes) {
            final int particaoTopico = p;
            final String topico = Wire.topicData(p);

            Thread thread = new Thread(() -> {
                while (!Thread.currentThread().isInterrupted()) {
                    try {
                        System.out.println("A subscrever: " + topico);
                        pubSub.subscribe(topico, mensagem ->
                                processarLote(mensagem, particaoTopico, ficheiro));
                    } catch (Exception e) {
                        System.out.println("Erro na subscricao " + topico
                                + ": " + e.getMessage());

                        try {
                            Thread.sleep(2000);
                        } catch (InterruptedException ex) {
                            Thread.currentThread().interrupt();
                        }
                    }
                }
            }, "replica-p" + p);
            thread.start();
        }

        // Thread de verificacao: check() em todos os monitores
        ScheduledExecutorService verificador =
                Executors.newSingleThreadScheduledExecutor(r -> {
                    Thread t = new Thread(r, "replica-verificacao");
                    t.setDaemon(true);
                    return t;
                });

        verificador.scheduleAtFixedRate(() -> {
            try {
                for (HeartbeatMonitor monitor : monitores) {
                    if (monitor != null) {
                        monitor.check();
                    }
                }
            } catch (Throwable e) {
                System.err.println("Erro na verificacao: " + e);
            }
        }, Wire.VERIFICACAO_MS, Wire.VERIFICACAO_MS, TimeUnit.MILLISECONDS);
    }

    // Escreve um evento na consola e em monitor-replica<r>.txt
    private static void evento(String texto) {
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

    // Formato dos registos no ficheiro da replica
    static String registoTexto(SensorRecord r) {
        return "REG | p=" + r.getPartition()
                + " | seq=" + r.getSequence()
                + " | sensor=" + r.getSensorId()
                + " | temp=" + r.getTemperature()
                + " | instante=" + r.getInstante()
                + " | L=" + r.getLamport()
                + " | V=" + Arrays.toString(r.getVectorClock());
    }

    // Chegou um LOTE: renova o detetor, aplica os registos pela ordem e,
    // se ficar aquem do ultimoSeq, recupera o que falta por TCP.
    private static void processarLote(String mensagem, int particao,
                                      RecordFile ficheiro) {
        String cabecalho = mensagem.trim().split("\\r?\\n", 2)[0];
        System.out.println("Mensagem recebida em p" + particao
                + ": " + cabecalho);

        Batch lote;

        try {
            lote = Batch.decode(mensagem, particao);
        } catch (IllegalArgumentException e) {
            System.out.println("LOTE invalido em p" + particao
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

        String host = lote.getAddress();
        int porta = lote.getPort();

        if (host.isBlank() || porta < 1 || porta > 65535) {
            System.out.println("Endereco ou porto invalido no LOTE.");
            return;
        }

        try {
            for (SensorRecord registo : registos) {
                if (!aplicar(registo, host, porta, particao, ficheiro)) {
                    // O ultimoSeq do lote seguinte voltara a tentar.
                    return;
                }
            }

            synchronized (ficheiro) {
                long ultimoAplicado = ficheiro.lastSeq(particao);

                if (ultimoAplicado < lote.getLastSeq()) {
                    long de = ultimoAplicado + 1;
                    long ate = lote.getLastSeq();

                    evento("RECUPERACAO;p" + particao
                            + ";de=" + de + ";ate=" + ate);

                    recuperarRegistos(host, porta, particao,
                            de, ate, ficheiro);
                }
            }
        } catch (IOException | IllegalArgumentException e) {
            System.out.println("Erro ao processar LOTE: " + e.getMessage());
        }
    }

    // Regra da COMINDIR02 para um registo: duplicados ignorados e lacunas
    // resolvidas por TCP antes de guardar. Devolve false se a recuperacao
    // das lacunas falhou (o registo nao e guardado).
    private static boolean aplicar(SensorRecord registo, String host,
                                   int porta, int particao,
                                   RecordFile ficheiro) throws IOException {
        long sequencia = registo.getSequence();

        if (sequencia < 1) {
            System.out.println("Sequencia invalida.");
            return true;
        }

        String reg = registoTexto(registo);

        synchronized (ficheiro) {
            if (ficheiro.contains(particao, sequencia)) {
                System.out.println("Registo duplicado. Ignorado.");
                return true;
            }

            long primeiraEmFalta =
                    ficheiro.firstMissing(particao, sequencia - 1);

            if (primeiraEmFalta < sequencia) {
                System.out.println("ATENCAO: registos em falta na particao "
                        + particao);
                long inicio = -1;
                for (long s = primeiraEmFalta; s < sequencia; s++) {
                    if (!ficheiro.contains(particao, s)) {
                        if (inicio == -1) inicio = s;
                    } else if (inicio != -1) {
                        if (!recuperarRegistos(host, porta, particao,
                                inicio, s - 1, ficheiro)) {
                            System.out.println(
                                    "Recuperacao incompleta. REG atual nao guardado.");
                            return false;
                        }
                        inicio = -1;
                    }
                }
                if (inicio != -1 && !recuperarRegistos(host, porta, particao,
                        inicio, sequencia - 1, ficheiro)) {
                    System.out.println(
                            "Recuperacao incompleta. REG atual nao guardado.");
                    return false;
                }
            }

            ficheiro.append(reg);
            System.out.println("Sequencia aceite: " + sequencia);
            System.out.println("Registo guardado no ficheiro.");
            return true;
        }
    }

    private static boolean recuperarRegistos(String host, int porta, int particao,
                                             long from, long to, RecordFile ficheiro) {
        System.out.println("A pedir recuperacao TCP a " + host + ":" + porta
                + " - PEDIDO " + from + " " + to);
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(host, porta), TIMEOUT_MS);
            socket.setSoTimeout(TIMEOUT_MS);
            BufferedReader in = new BufferedReader(new InputStreamReader(
                    socket.getInputStream(), StandardCharsets.UTF_8));
            PrintWriter out = new PrintWriter(new OutputStreamWriter(
                    socket.getOutputStream(), StandardCharsets.UTF_8), true);
            out.println("PEDIDO " + from + " " + to);
            long esperado = from;
            int recuperados = 0;
            boolean recebeuFim = false;
            String linha;
            while ((linha = in.readLine()) != null) {
                if (linha.equals("FIM")) {
                    recebeuFim = true;
                    break;
                }
                if (linha.startsWith("ERRO")) {
                    System.out.println("Erro do lider: " + linha);
                    return false;
                }

                // seq;sensor;temperatura;instante;L;V (a particao vem do pedido)
                SensorRecord registo = SensorRecord.fromLine(linha, particao);
                long seq = registo.getSequence();

                if (seq != esperado || seq > to) {
                    System.out.println("Sequencia inesperada na recuperacao: " + seq);
                    return false;
                }

                if (!ficheiro.contains(particao, seq)) {
                    ficheiro.append(registoTexto(registo));
                    recuperados++;
                    System.out.println("Registo recuperado e guardado: seq=" + seq);
                }
                esperado++;
            }
            boolean completo = recebeuFim && esperado == to + 1;
            System.out.println(completo ? "Recuperacao TCP concluida: " + recuperados + " registos."
                    : "Recuperacao TCP incompleta.");
            return completo;
        } catch (IOException | IllegalArgumentException e) {
            System.out.println("Erro na recuperacao TCP: " + e.getMessage());
            return false;
        }
    }
}
