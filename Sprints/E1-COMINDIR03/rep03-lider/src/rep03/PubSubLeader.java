


        package rep03;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
import java.util.Random;
import java.util.Scanner;
import java.util.concurrent.atomic.AtomicLong;

public class PubSubLeader {

    static final String DEFAULT_API = "http://127.0.0.1:5001/api/v0";
    static final int PARTITIONS = 3;

    public static void main(String[] args) {

        int partition = args.length > 0
                ? Integer.parseInt(args[0]) : 0;

        String api = args.length > 1
                ? args[1] : DEFAULT_API;

        if (partition < 0 || partition >= PARTITIONS) {
            System.out.println("Particao invalida.");
            return;
        }

        IpfsPubSub pubSub = new IpfsPubSub(api);
        LamportClock lamport = new LamportClock();
        VectorClock vector = new VectorClock(PARTITIONS);

        RecordFile file = new RecordFile(
                "lider-p" + partition + ".txt");

        int tcpPort = Wire.PORTO_BASE + partition;
        int producerPort = 7100 + partition;

        // Sequencia partilhada pela consola e pelo produtor
        AtomicLong seq;

        try {
            SensorRecord ultimo = file.lastRecord(partition);

            if (ultimo == null) {
                seq = new AtomicLong(0);
            } else {
                seq = new AtomicLong(ultimo.getSequence());

                // Recupera o relogio de Lamport
                // FALHA DELIBERADA 3:
// Recupera a sequencia, mas nao recupera L nem V.

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

        // Servidor TCP para recuperacao de registos
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
                        processIncoming(partition, seq, mensagem,
                                lamport, vector, file, pubSub, tcpPort);
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
                                publishRecord(
                                        partition,
                                        seq,
                                        partes[0],
                                        temperatura,
                                        lamport,
                                        vector,
                                        file,
                                        pubSub
                                );

                                out.println("OK");

                            } catch (Exception e) {
                                out.println("ERRO publicacao");
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
                    int publicados = 0;

                    for (int i = 0; i < n; i++) {
                        double temp = 15 + random.nextDouble() * 25;

                        try {
                            publishRecord(
                                    partition,
                                    seq,
                                    "SIM",
                                    temp,
                                    lamport,
                                    vector,
                                    file,
                                    pubSub
                            );
                            publicados++;

                            // Pausa apenas entre publicacoes.
                            if (i < n - 1 && intervalo > 0) {
                                Thread.sleep(intervalo);
                            }
                        } catch (IOException | InterruptedException e) {
                            System.out.println(
                                    "Erro na publicacao: " + e.getMessage());
                            if (e instanceof InterruptedException) {
                                Thread.currentThread().interrupt();
                            }
                            break;
                        }
                    }

                    long duration = System.currentTimeMillis() - start;
                    System.out.println("Rajada concluida: " + publicados + " registos");
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
                    publishRecord(
                            partition,
                            seq,
                            parts[0],
                            temperature,
                            lamport,
                            vector,
                            file,
                            pubSub
                    );

                } catch (IOException | InterruptedException e) {
                    System.out.println(
                            "Erro: " + e.getMessage());

                    if (e instanceof InterruptedException) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }
        }
    }

    // Recebe IN;sensor;temperatura;L;v0,v1,v2 e gera um REG.
    private static void processIncoming(
            int partition, AtomicLong seq, String message,
            LamportClock lamport, VectorClock vector,
            RecordFile file, IpfsPubSub pubSub, int tcpPort)
            throws IOException, InterruptedException {

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

        publishRecord(partition, seq, sensor, temperature, lamport,
                vector, file, pubSub, receivedLamport, receivedVector);
    }

    // Secao critica partilhada pela consola, TCP e mensagens IN.
    private static synchronized void publishRecord(
            int partition, AtomicLong seq, String sensorId,
            double temperature, LamportClock lamport,
            VectorClock vector, RecordFile file, IpfsPubSub pubSub,
            int receivedLamport, int[] receivedVector)
            throws IOException, InterruptedException {

        // FALHA DELIBERADA 2: ignora o Lamport recebido
        int logicalTime = lamport.update(receivedLamport);
        vector.merge(receivedVector);
        vector.increment(partition);

        long nextSeq = seq.get() + 1;
        SensorRecord record = new SensorRecord(partition, nextSeq,
                sensorId, temperature, logicalTime, vector.getClock());
        file.append(record);
        seq.set(nextSeq);
        pubSub.publish(Wire.topicData(partition),
                Wire.reg("127.0.0.1", Wire.PORTO_BASE + partition, record));
        System.out.println("Publicado a partir de IN: " + record);
    }

    // Cria, guarda e publica um registo
    // synchronized impede publicacoes simultaneas
    private static synchronized void publishRecord(
            int partition,
            AtomicLong seq,
            String sensorId,
            double temperature,
            LamportClock lamport,
            VectorClock vector,
            RecordFile file,
            IpfsPubSub pubSub)
            throws IOException, InterruptedException {

        int logicalTime = lamport.tick();

        vector.increment(partition);

        long nextSeq = seq.get() + 1;

        SensorRecord record = new SensorRecord(
                partition,
                nextSeq,
                sensorId,
                temperature,
                logicalTime,
                vector.getClock()
        );

        // Guarda primeiro no ficheiro
        file.append(record);

        // Atualiza a sequencia depois de guardar
        seq.set(nextSeq);

        // Publica no IPFS
        pubSub.publish(Wire.topicData(partition),
                Wire.reg("127.0.0.1", Wire.PORTO_BASE + partition, record));

        System.out.println("Publicado: " + record);
    }
}


