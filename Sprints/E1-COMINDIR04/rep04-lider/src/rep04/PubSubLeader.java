package rep04;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Scanner;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class PubSubLeader {

    static final String DEFAULT_API = "http://127.0.0.1:5001/api/v0";
    static final int PARTITIONS = Wire.PARTITIONS;
    static final String HOST = "127.0.0.1";

    // Monitor unico da seccao critica: atribuicao de seq, L, V, append,
    // lista pendentes, hb e leitura de ultimoSeq
    private static final Object LOCK = new Object();

    // Estado do lider (inicializado em main antes de arrancar as threads)
    private static int partition;
    private static int tcpPort;
    private static IpfsPubSub pubSub;
    private static LamportClock lamport;
    private static VectorClock vector;
    private static RecordFile file;
    private static AtomicLong seq;
    private static Path hbFile;

    // Incarnacao: instante de arranque em ms
    private static long inc;

    // Protegidos por LOCK
    private static List<SensorRecord> pendentes = new ArrayList<>();
    private static long hb = 0;

    public static void main(String[] args) {

        partition = args.length > 0
                ? Integer.parseInt(args[0]) : 0;

        String api = args.length > 1
                ? args[1] : DEFAULT_API;

        if (partition < 0 || partition >= PARTITIONS) {
            System.out.println("Particao invalida.");
            return;
        }

        pubSub = new IpfsPubSub(api);
        lamport = new LamportClock();
        vector = new VectorClock(PARTITIONS);

        file = new RecordFile("lider-p" + partition + ".txt");
        hbFile = Paths.get("hb-p" + partition + ".txt");

        tcpPort = Wire.PORTO_BASE + partition;
        int producerPort = 7100 + partition;

        inc = System.currentTimeMillis();
        hb = 0;

        try {
            SensorRecord ultimo = file.lastRecord(partition);

            if (ultimo == null) {
                seq = new AtomicLong(0);
            } else {
                seq = new AtomicLong(ultimo.getSequence());

                // Recupera o relogio de Lamport
                lamport.restore(ultimo.getLamport());

                // Adapta vetores antigos de 2 posicoes para 3
                int[] vetorGuardado = ultimo.getVectorClock();
                int[] vetorRecuperado = new int[PARTITIONS];

                System.arraycopy(
                        vetorGuardado, 0,
                        vetorRecuperado, 0,
                        Math.min(vetorGuardado.length, PARTITIONS)
                );

                vector.merge(vetorRecuperado);

                System.out.println(
                        "Estado recuperado: seq=" + seq.get()
                                + " L=" + lamport.getClock()
                                + " V=" + vector);
            }
        } catch (IOException | IllegalArgumentException e) {
            System.out.println(
                    "Erro ao recuperar estado: " + e.getMessage());
            return;
        }

        System.out.println("Incarnacao inc=" + inc
                + " PERIODO_HB_MS=" + Wire.PERIODO_HB_MS);

        // Servidor TCP para recuperacao de registos (inalterado)
        Thread tcpThread = new Thread(() -> {

            try (ServerSocket server = new ServerSocket(tcpPort)) {

                System.out.println(
                        "Servidor TCP pronto na porta " + tcpPort);

                while (!Thread.currentThread().isInterrupted()) {

                    try (Socket socket = server.accept()) {

                        socket.setSoTimeout(Wire.T_TCP_MS);

                        BufferedReader in = new BufferedReader(
                                new InputStreamReader(
                                        socket.getInputStream()));

                        PrintWriter out = new PrintWriter(
                                socket.getOutputStream(), true);

                        String pedido = in.readLine();

                        if (pedido == null) {
                            continue;
                        }

                        System.out.println(
                                "Pedido TCP recebido: " + pedido);

                        String[] partes = pedido.trim().split("\\s+");

                        if (partes.length != 3
                                || !partes[0].equals("PEDIDO")) {

                            out.println("ERRO pedido invalido");
                            continue;
                        }

                        long from = Long.parseLong(partes[1]);
                        long to = Long.parseLong(partes[2]);

                        if (from < 1 || from > to) {
                            out.println("ERRO intervalo invalido");
                            continue;
                        }

                        List<SensorRecord> registos =
                                file.range(partition, from, to);

                        for (SensorRecord registo : registos) {
                            out.println(registo.toLine());
                        }

                        out.println("FIM");

                        System.out.println(
                                "Recuperacao TCP concluida: "
                                        + registos.size() + " registos");

                    } catch (Exception e) {
                        System.out.println(
                                "Erro num pedido TCP: " + e.getMessage());
                    }
                }

            } catch (IOException e) {
                System.out.println(
                        "Erro no servidor TCP: " + e.getMessage());
            }
        });

        tcpThread.setDaemon(true);
        tcpThread.start();

        // Subscreve as mensagens IN da particao
        Thread subscriptionThread = new Thread(() -> {
            String topic = Wire.topicIn(partition);

            System.out.println(
                    "A subscrever mensagens IN no topico: " + topic);

            try {
                pubSub.subscribe(topic, mensagem -> {
                    System.out.println("Mensagem IN recebida: " + mensagem);
                    try {
                        processIncoming(mensagem);
                    } catch (Exception e) {
                        System.out.println("Erro ao processar IN: " + e.getMessage());
                    }
                });

            } catch (Exception e) {
                System.out.println(
                        "Erro na subscricao IN: " + e.getMessage());
            }
        });

        subscriptionThread.setDaemon(true);
        subscriptionThread.start();

        // Servidor TCP para receber temperaturas do produtor
        Thread producerThread = new Thread(() -> {

            try (ServerSocket server =
                         new ServerSocket(producerPort)) {

                System.out.println(
                        "Servidor do produtor pronto na porta "
                                + producerPort);

                while (!Thread.currentThread().isInterrupted()) {

                    try (Socket socket = server.accept()) {

                        socket.setSoTimeout(Wire.T_TCP_MS);

                        BufferedReader in = new BufferedReader(
                                new InputStreamReader(
                                        socket.getInputStream()));

                        PrintWriter out = new PrintWriter(
                                socket.getOutputStream(), true);

                        String linha;

                        while ((linha = in.readLine()) != null) {

                            String[] partes =
                                    linha.trim().split("\\s+");

                            if (partes.length != 2) {
                                out.println("ERRO formato");
                                continue;
                            }

                            double temperatura;

                            try {
                                temperatura =
                                        Double.parseDouble(partes[1]);

                                if (!Double.isFinite(temperatura)) {
                                    out.println("ERRO temperatura");
                                    continue;
                                }

                            } catch (NumberFormatException e) {
                                out.println("ERRO temperatura");
                                continue;
                            }

                            try {
                                registarLocal(partes[0], temperatura);
                                out.println("OK");

                            } catch (Exception e) {
                                out.println("ERRO registo");
                                System.out.println(
                                        "Erro no produtor: "
                                                + e.getMessage());
                            }
                        }

                    } catch (Exception e) {
                        System.out.println(
                                "Erro na ligacao do produtor: "
                                        + e.getMessage());
                    }
                }

            } catch (IOException e) {
                System.out.println(
                        "Erro no servidor do produtor: "
                                + e.getMessage());
            }
        });

        producerThread.setDaemon(true);
        producerThread.start();

        // Heartbeat/lote: no arranque e depois a cada PERIODO_HB_MS
        ScheduledExecutorService scheduler =
                Executors.newSingleThreadScheduledExecutor(r -> {
                    Thread t = new Thread(r, "heartbeat-p" + partition);
                    t.setDaemon(true);
                    return t;
                });

        scheduler.scheduleAtFixedRate(
                PubSubLeader::enviarLote,
                0, Wire.PERIODO_HB_MS, TimeUnit.MILLISECONDS);

        // Continua a aceitar comandos pela consola
        try (Scanner sc = new Scanner(System.in)) {

            System.out.println(
                    "Lider da particao " + partition + " pronto.");

            System.out.println(
                    "Formato: <sensor> <temperatura>");

            System.out.println(
                    "Comandos: rajada <n> [intervalo_ms] | sair");

            Random random = new Random();

            while (true) {

                System.out.print("> ");

                if (!sc.hasNextLine()) {
                    break;
                }

                String line = sc.nextLine().trim();

                if (line.equalsIgnoreCase("sair")) {
                    break;
                }

                if (line.startsWith("rajada ")) {

                    String[] parts = line.split("\\s+");

                    if (parts.length != 2 && parts.length != 3) {
                        System.out.println("Formato: rajada <n> [intervalo_ms]");
                        continue;
                    }

                    int n;
                    int intervalo;

                    try {
                        n = Integer.parseInt(parts[1]);
                        intervalo = parts.length == 3
                                ? Integer.parseInt(parts[2]) : 0;

                        if (n <= 0 || intervalo < 0) {
                            System.out.println("Numero ou intervalo invalido.");
                            continue;
                        }
                    } catch (NumberFormatException e) {
                        System.out.println("Numero ou intervalo invalido.");
                        continue;
                    }

                    long start = System.currentTimeMillis();
                    int registados = 0;

                    for (int i = 0; i < n; i++) {
                        double temp = 15 + random.nextDouble() * 25;

                        try {
                            registarLocal("SIM", temp);
                            registados++;

                            // Pausa apenas entre registos.
                            if (i < n - 1 && intervalo > 0) {
                                Thread.sleep(intervalo);
                            }
                        } catch (IOException | InterruptedException e) {
                            System.out.println(
                                    "Erro no registo: " + e.getMessage());
                            if (e instanceof InterruptedException) {
                                Thread.currentThread().interrupt();
                            }
                            break;
                        }
                    }

                    long duration = System.currentTimeMillis() - start;
                    System.out.println("Rajada concluida: " + registados + " registos");
                    System.out.println("Intervalo configurado: " + intervalo + " ms");
                    System.out.println("Duracao: " + duration + " ms");
                    continue;
                }

                String[] parts = line.split("\\s+");

                if (parts.length != 2) {
                    System.out.println("Formato invalido.");
                    continue;
                }

                double temperature;

                try {
                    temperature = Double.parseDouble(parts[1]);

                    if (!Double.isFinite(temperature)) {
                        System.out.println("Temperatura invalida.");
                        continue;
                    }

                } catch (NumberFormatException e) {
                    System.out.println("Temperatura invalida.");
                    continue;
                }

                try {
                    registarLocal(parts[0], temperature);

                } catch (IOException e) {
                    System.out.println(
                            "Erro: " + e.getMessage());
                }
            }
        }

        scheduler.shutdownNow();
    }

    // Recebe IN;sensor;temperatura;L;v0,v1,v2 e regista a leitura.
    private static void processIncoming(String message) throws IOException {

        String[] fields = message.trim().split(";", -1);
        if (fields.length != 5 || !"IN".equals(fields[0])) {
            throw new IllegalArgumentException("Formato IN invalido: " + message);
        }

        String sensor = fields[1].trim();
        if (sensor.isEmpty()) {
            throw new IllegalArgumentException("Sensor vazio");
        }

        double temperature = Double.parseDouble(fields[2].trim());
        if (!Double.isFinite(temperature)) {
            throw new IllegalArgumentException("Temperatura invalida");
        }

        int receivedLamport = Integer.parseInt(fields[3].trim());
        if (receivedLamport < 0) {
            throw new IllegalArgumentException("Lamport negativo");
        }

        String[] vectorFields = fields[4].split(",", -1);
        if (vectorFields.length != PARTITIONS) {
            throw new IllegalArgumentException("Vetor deve ter 3 posicoes");
        }
        int[] receivedVector = new int[PARTITIONS];
        for (int i = 0; i < PARTITIONS; i++) {
            receivedVector[i] = Integer.parseInt(vectorFields[i].trim());
            if (receivedVector[i] < 0) {
                throw new IllegalArgumentException("Vetor negativo");
            }
        }

        registarIn(sensor, temperature, receivedLamport, receivedVector);
    }

    // Leitura vinda de IN: L = max(L, recebido) + 1; V = max(V, recebido); V[k]++
    private static void registarIn(String sensorId, double temperature,
                                   int receivedLamport, int[] receivedVector)
            throws IOException {

        synchronized (LOCK) {
            int logicalTime = lamport.update(receivedLamport);
            vector.merge(receivedVector);
            vector.increment(partition);

            atribuirEAcrescentar(sensorId, temperature, logicalTime);
        }
    }

    // Leitura local (consola ou ligacao TCP do produtor): L++ ; V[k]++
    private static void registarLocal(String sensorId, double temperature)
            throws IOException {

        synchronized (LOCK) {
            int logicalTime = lamport.tick();
            vector.increment(partition);

            atribuirEAcrescentar(sensorId, temperature, logicalTime);
        }
    }

    // Chamar apenas com LOCK detido: atribui seq, verifica V[k] = seq,
    // guarda no ficheiro e acrescenta a pendentes. NAO publica.
    private static void atribuirEAcrescentar(String sensorId,
                                             double temperature,
                                             int logicalTime)
            throws IOException {

        long nextSeq = seq.get() + 1;
        int[] v = vector.getClock();

        if (v[partition] != nextSeq) {
            throw new IllegalStateException(
                    "Invariante violada: V[" + partition + "]="
                            + v[partition] + " seq=" + nextSeq);
        }

        SensorRecord record = new SensorRecord(
                partition,
                nextSeq,
                sensorId,
                temperature,
                logicalTime,
                v
        );

        // Guarda primeiro no ficheiro
        file.append(record);

        // Atualiza a sequencia depois de guardar
        seq.set(nextSeq);

        // Fica a aguardar o proximo heartbeat
        pendentes.add(record);

        System.out.println("Registado (pendente): " + record);
    }

    // Fecha o lote atual e publica-o; serve de heartbeat mesmo se vazio.
    // Executada pelo scheduler; nunca deixa escapar excecoes, pois
    // scheduleAtFixedRate suspenderia as execucoes seguintes.
    private static void enviarLote() {

        List<SensorRecord> lote;
        long ultimoSeq;
        long hbAtual;

        synchronized (LOCK) {
            lote = pendentes;
            pendentes = new ArrayList<>();
            ultimoSeq = seq.get();
            hbAtual = ++hb;
        }

        // Fora da seccao sincronizada: publicar e lento e bloquearia
        // a atribuicao de seq a novas leituras.
        try {
            Batch batch = new Batch(HOST, tcpPort, inc, hbAtual,
                    ultimoSeq, lote);

            String mensagem = batch.encode();
            int bytes = mensagem.getBytes(StandardCharsets.UTF_8).length;

            pubSub.publish(Wire.topicData(partition), mensagem);

            long instante = System.currentTimeMillis();

            Files.writeString(
                    hbFile,
                    inc + ";" + hbAtual + ";" + instante + ";"
                            + ultimoSeq + ";" + lote.size() + ";" + bytes
                            + System.lineSeparator(),
                    StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.APPEND);

            System.out.println("HEARTBEAT: hb=" + hbAtual
                    + " n=" + lote.size()
                    + " ultimoSeq=" + ultimoSeq);

        } catch (Throwable e) {
            // Os registos do lote nao sao republicados: os participantes
            // recuperam-nos pelo ultimoSeq do lote seguinte.
            System.out.println("ERRO ao publicar lote hb=" + hbAtual
                    + " (n=" + lote.size() + "): " + e);
        }
    }
}
