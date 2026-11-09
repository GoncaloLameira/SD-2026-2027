
package raft04;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Locale;
import java.util.Random;
import java.util.Scanner;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class PubSubProdutor {

    private static final String DEFAULT_API =
            "http://127.0.0.1:5001/api/v0";

    private static final int PARTITIONS = Wire.PARTITIONS;

    private final IpfsPubSub pubSub;
    private final Partitioner partitioner = new Partitioner(PARTITIONS);
    private final Random random = new Random();

    // Um detetor de falhas por particao.
    private final HeartbeatMonitor[] monitores =
            new HeartbeatMonitor[PARTITIONS];

    private final Path ficheiroMonitor =
            Paths.get("monitor-produtor.txt");

    private final Object logLock = new Object();

    public PubSubProdutor(String api) {
        pubSub = new IpfsPubSub(api);

        for (int k = 0; k < PARTITIONS; k++) {
            monitores[k] = new HeartbeatMonitor(
                    k,
                    Wire.PERIODO_HB_MS,
                    Wire.FATOR_TIMEOUT,
                    this::evento
            );
        }
    }

    public static void main(String[] args) {
        String api = args.length > 0 ? args[0] : DEFAULT_API;

        PubSubProdutor produtor = new PubSubProdutor(api);
        produtor.iniciarDetetores();
        produtor.consola();
    }

    // Subscreve os topicos LOTE das tres particoes.
    private void iniciarDetetores() {

        for (int k = 0; k < PARTITIONS; k++) {

            final int particao = k + 1;

            Thread thread = new Thread(() -> {

                while (!Thread.currentThread().isInterrupted()) {
                    try {
                        pubSub.subscribe(
                                Wire.topicData(particao),
                                mensagem -> aoReceberLote(
                                        mensagem, particao)
                        );

                    } catch (Exception e) {
                        System.err.println(
                                "Erro na subscricao de p"
                                        + particao + ": "
                                        + e.getMessage()
                        );

                        try {
                            Thread.sleep(2000);
                        } catch (InterruptedException ex) {
                            Thread.currentThread().interrupt();
                        }
                    }
                }

            }, "Produtor-p" + particao);

            thread.setDaemon(true);
            thread.start();
        }

        ScheduledExecutorService verificador =
                Executors.newSingleThreadScheduledExecutor(r -> {
                    Thread t = new Thread(
                            r, "Produtor-verificacao");
                    t.setDaemon(true);
                    return t;
                });

        verificador.scheduleAtFixedRate(() -> {
                    try {
                        for (HeartbeatMonitor monitor : monitores) {
                            monitor.check();
                        }
                    } catch (Throwable e) {
                        System.err.println(
                                "Erro na verificacao: " + e);
                    }
                },
                Wire.VERIFICACAO_MS,
                Wire.VERIFICACAO_MS,
                TimeUnit.MILLISECONDS
        );
    }

    // Regista eventos na consola e em monitor-produtor.txt.
    private void evento(String texto) {

        String linha =
                System.currentTimeMillis() + ";" + texto;

        synchronized (logLock) {
            System.out.println("[monitor] " + linha);

            try {
                Files.writeString(
                        ficheiroMonitor,
                        linha + System.lineSeparator(),
                        StandardCharsets.UTF_8,
                        StandardOpenOption.CREATE,
                        StandardOpenOption.APPEND
                );
            } catch (IOException e) {
                System.err.println(
                        "Erro ao escrever o monitor: "
                                + e.getMessage());
            }
        }
    }

    // Formato:
    // LOTE;term;leader;address;port;hb;lastSeq;lastTerm;n
    private void aoReceberLote(
            String mensagem, int particao) {

        String cabecalho =
                mensagem.trim().split("\\r?\\n", 2)[0];

        String[] h = cabecalho.trim().split(";", -1);

        if (h.length != 9 || !h[0].equals("LOTE")) {
            System.err.println(
                    "LOTE invalido em p" + particao
                            + ": " + cabecalho);
            return;
        }

        long term;
        int leader;
        long hb;
        long ultimoSeq;
        long ultimoTerm;
        int n;

        try {
            term = Long.parseLong(h[1].trim());
            leader = Integer.parseInt(h[2].trim());

            String address = h[3].trim();

            int port = Integer.parseInt(h[4].trim());
            hb = Long.parseLong(h[5].trim());
            ultimoSeq = Long.parseLong(h[6].trim());
            ultimoTerm = Long.parseLong(h[7].trim());
            n = Integer.parseInt(h[8].trim());

            if (term < 1
                    || leader < 1
                    || leader > Wire.GRUPO
                    || address.isBlank()
                    || port < 1
                    || port > 65535
                    || hb < 1
                    || ultimoSeq < 0
                    || ultimoTerm < 0
                    || n < 0) {

                throw new NumberFormatException(
                        "Valores invalidos");
            }

        } catch (NumberFormatException e) {
            System.err.println(
                    "Cabecalho LOTE invalido em p"
                            + particao + ": " + cabecalho);
            return;
        }

        // O array usa indices 0, 1 e 2.
        monitores[particao - 1].onHeartbeat(
                term, leader, hb);

        evento("RECEBIDO;p" + particao
                + ";term=" + term
                + ";lider=" + leader
                + ";hb=" + hb
                + ";ultimoSeq=" + ultimoSeq
                + ";ultimoTerm=" + ultimoTerm
                + ";n=" + n);
    }

    // Envia uma leitura para a particao correspondente ao sensor.
    private void enviar(
            String sensor, String mensagem)
            throws IOException, InterruptedException {

        // As particoes dos topicos sao numeradas de 1 a 3.
        int particao = partitioner.of(sensor) + 1;

        String topico = Wire.topicIn(particao);

        if (monitores[particao - 1].state()
                == HeartbeatMonitor.State.SUSPEITO) {

            System.out.println(
                    "AVISO: partição p" + particao
                            + " sem líder conhecido; leitura "
                            + sensor + " pode perder-se");
        }

        // Mesmo sem lider conhecido, a leitura e enviada.
        pubSub.publish(topico, mensagem);

        System.out.println(
                "Publicado: " + mensagem
                        + " | particao=" + particao
                        + " | topico=" + topico);

        // Regista cada leitura enviada.
        String linha = System.currentTimeMillis()
                + ";" + particao
                + ";" + sensor
                + ";" + mensagem.split(";")[2]
                + System.lineSeparator();

        synchronized (logLock) {
            Files.writeString(
                    Paths.get("produtor.txt"),
                    linha,
                    StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.APPEND
            );
        }
    }

    // Gera uma leitura com temperatura aleatoria.
    private String leituraAleatoria(String sensor) {

        double temperatura = 15 + random.nextDouble() * 25;

        return String.format(
                Locale.US,
                "IN;%s;%.2f;0;0,0,0",
                sensor,
                temperatura
        );
    }

    private void consola() {

        System.out.println("Produtor iniciado.");
        System.out.println("Particoes: " + PARTITIONS);
        System.out.println("Formato: <sensor> <temperatura>");
        System.out.println(
                "Comandos: rajada <n> [intervalo_ms] | auto | sair"
        );

        try (Scanner sc = new Scanner(System.in)) {

            while (true) {

                System.out.print("> ");

                if (!sc.hasNextLine()) {
                    break;
                }

                String line = sc.nextLine().trim();

                if (line.isEmpty()) {
                    continue;
                }

                if (line.equalsIgnoreCase("sair")) {
                    break;
                }

                String[] parts = line.split("\\s+");

                try {
                    if (parts[0].equals("auto")
                            && parts.length == 1) {

                        auto();

                    } else if (parts[0].equals("rajada")) {

                        rajada(parts);

                    } else {

                        leitura(parts);
                    }

                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;

                } catch (Exception e) {
                    System.err.println(
                            "Erro ao publicar leitura: "
                                    + e.getMessage());
                }
            }
        }

        System.out.println("Produtor terminado.");
    }

    // Comando: <sensor> <temperatura>
    private void leitura(String[] parts)
            throws IOException, InterruptedException {

        if (parts.length != 2) {
            System.out.println("Formato invalido.");
            return;
        }

        double temperatura;

        try {
            temperatura = Double.parseDouble(parts[1]);

            if (!Double.isFinite(temperatura)) {
                System.out.println("Temperatura invalida.");
                return;
            }

        } catch (NumberFormatException e) {
            System.out.println("Temperatura invalida.");
            return;
        }

        enviar(
                parts[0],
                Wire.in(parts[0], temperatura)
        );
    }

    // Comando: rajada <n> [intervalo_ms]
    private void rajada(String[] parts)
            throws InterruptedException {

        if (parts.length != 2 && parts.length != 3) {
            System.out.println(
                    "Formato: rajada <n> [intervalo_ms]");
            return;
        }

        int n;
        int intervalo;

        try {
            n = Integer.parseInt(parts[1]);

            intervalo = parts.length == 3
                    ? Integer.parseInt(parts[2])
                    : 0;

            if (n <= 0 || intervalo < 0) {
                System.out.println(
                        "Numero ou intervalo invalido.");
                return;
            }

        } catch (NumberFormatException e) {
            System.out.println(
                    "Numero ou intervalo invalido.");
            return;
        }

        long inicio = System.currentTimeMillis();
        int enviados = 0;

        for (int i = 0; i < n; i++) {

            String sensor =
                    "sensorAuto" + (i % 10 + 1);

            try {
                enviar(sensor, leituraAleatoria(sensor));
                enviados++;

            } catch (IOException e) {
                System.err.println(
                        "Erro ao publicar leitura: "
                                + e.getMessage());
            }

            if (i < n - 1 && intervalo > 0) {
                Thread.sleep(intervalo);
            }
        }

        System.out.println(
                "Rajada concluida: " + enviados + " leituras");

        System.out.println(
                "Intervalo configurado: " + intervalo + " ms");

        System.out.println(
                "Duracao: "
                        + (System.currentTimeMillis() - inicio)
                        + " ms");
    }

    // Dez leituras, com um segundo entre elas.
    private void auto() throws InterruptedException {

        for (int i = 1; i <= 10; i++) {

            String sensor = "sensorAuto" + i;

            try {
                enviar(sensor, leituraAleatoria(sensor));

            } catch (IOException e) {
                System.err.println(
                        "Erro ao publicar leitura: "
                                + e.getMessage());
            }

            Thread.sleep(1000);
        }
    }
}
