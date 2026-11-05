package rep04;

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

    // Um detetor de falhas por particao, alimentado pelos cabecalhos dos LOTE
    private final HeartbeatMonitor[] monitores =
            new HeartbeatMonitor[PARTITIONS];

    // Eventos do detetor: instante;evento;p<k>;detalhe
    private final Path ficheiroMonitor = Paths.get("monitor-produtor.txt");
    private final Object logLock = new Object();

    public PubSubProdutor(String api) {
        pubSub = new IpfsPubSub(api);

        for (int k = 0; k < PARTITIONS; k++) {
            monitores[k] = new HeartbeatMonitor(
                    k, Wire.PERIODO_HB_MS, Wire.FATOR_TIMEOUT,
                    this::evento);
        }
    }

    public static void main(String[] args) {
        String api = args.length > 0 ? args[0] : DEFAULT_API;

        PubSubProdutor produtor = new PubSubProdutor(api);
        produtor.iniciarDetetores();
        produtor.consola();
    }

    // Subscreve os topicos LOTE de todas as particoes e arranca a verificacao
    private void iniciarDetetores() {
        for (int k = 0; k < PARTITIONS; k++) {
            final int particao = k;

            Thread thread = new Thread(() -> {
                while (!Thread.currentThread().isInterrupted()) {
                    try {
                        pubSub.subscribe(Wire.topicData(particao),
                                mensagem -> aoReceberLote(mensagem, particao));

                    } catch (Exception e) {
                        System.err.println("Erro na subscricao de p"
                                + particao + ": " + e.getMessage());

                        try {
                            Thread.sleep(2000);
                        } catch (InterruptedException ex) {
                            Thread.currentThread().interrupt();
                        }
                    }
                }
            }, "Produtor-p" + k);

            thread.setDaemon(true);
            thread.start();
        }

        ScheduledExecutorService verificador =
                Executors.newSingleThreadScheduledExecutor(r -> {
                    Thread t = new Thread(r, "Produtor-verificacao");
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
    }

    // Escreve um evento na consola e em monitor-produtor.txt
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

    // O produtor so interpreta o cabecalho:
    // LOTE;<endereco>;<porto>;<inc>;<hb>;<ultimoSeq>;<n>
    private void aoReceberLote(String mensagem, int particao) {
        String cabecalho = mensagem.trim().split("\\r?\\n", 2)[0];
        String[] h = cabecalho.trim().split(";", -1);

        if (h.length != 7 || !h[0].equals("LOTE")) {
            System.err.println("LOTE invalido em p" + particao
                    + ": " + cabecalho);
            return;
        }

        long inc;
        long hb;
        long ultimoSeq;
        int n;

        try {
            inc = Long.parseLong(h[3].trim());
            hb = Long.parseLong(h[4].trim());
            ultimoSeq = Long.parseLong(h[5].trim());
            n = Integer.parseInt(h[6].trim());
        } catch (NumberFormatException e) {
            System.err.println("Cabecalho LOTE invalido em p" + particao
                    + ": " + cabecalho);
            return;
        }

        monitores[particao].onHeartbeat(inc, hb);

        evento("RECEBIDO;p" + particao
                + ";inc=" + inc
                + ";hb=" + hb
                + ";ultimoSeq=" + ultimoSeq
                + ";n=" + n);
    }

    // Envia um IN para o lider da particao do sensor. Se o detetor suspeita
    // desse lider, avisa e envia na mesma.
    private void enviar(String sensor, String mensagem)
            throws IOException, InterruptedException {

        int particao = partitioner.of(sensor);
        String topico = Wire.topicIn(particao);

        if (monitores[particao].state() == HeartbeatMonitor.State.SUSPEITO) {
            System.out.println(
                    "AVISO: líder p" + particao
                            + " suspeito; leitura " + sensor
                            + " pode perder-se");
        }

        pubSub.publish(topico, mensagem);

        System.out.println(
                "Publicado: " + mensagem
                        + " | particao=" + particao
                        + " | topico=" + topico);
    }

    // IN com temperatura aleatoria, com duas casas decimais
    private String leituraAleatoria(String sensor) {
        double temperatura = 15 + random.nextDouble() * 25;

        return String.format(
                Locale.US,
                "IN;%s;%.2f;0;0,0,0",
                sensor,
                temperatura);
    }

    private void consola() {
        System.out.println("Produtor iniciado.");
        System.out.println("Particoes: " + PARTITIONS);
        System.out.println("Formato: <sensor> <temperatura>");
        System.out.println(
                "Comandos: rajada <n> [intervalo_ms] | auto | sair");

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
                    if (parts[0].equals("auto") && parts.length == 1) {
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
                            "Erro ao publicar leitura: " + e.getMessage());
                }
            }
        }

        System.out.println("Produtor terminado.");
    }

    // <sensor> <temperatura>
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

        enviar(parts[0], Wire.in(parts[0], temperatura));
    }

    // rajada <n> [intervalo_ms]: leituras de sensorAuto1..10, repartidas
    // pelas particoes segundo o Partitioner
    private void rajada(String[] parts) throws InterruptedException {

        if (parts.length != 2 && parts.length != 3) {
            System.out.println("Formato: rajada <n> [intervalo_ms]");
            return;
        }

        int n;
        int intervalo;

        try {
            n = Integer.parseInt(parts[1]);
            intervalo = parts.length == 3
                    ? Integer.parseInt(parts[2]) : 0;

            if (n <= 0 || intervalo < 0) {
                System.out.println("Numero ou intervalo invalido.");
                return;
            }
        } catch (NumberFormatException e) {
            System.out.println("Numero ou intervalo invalido.");
            return;
        }

        long inicio = System.currentTimeMillis();
        int enviados = 0;

        for (int i = 0; i < n; i++) {
            String sensor = "sensorAuto" + (i % 10 + 1);

            try {
                enviar(sensor, leituraAleatoria(sensor));
                enviados++;
            } catch (IOException e) {
                System.err.println(
                        "Erro ao publicar leitura: " + e.getMessage());
            }

            if (i < n - 1 && intervalo > 0) {
                Thread.sleep(intervalo);
            }
        }

        System.out.println("Rajada concluida: " + enviados + " leituras");
        System.out.println("Intervalo configurado: " + intervalo + " ms");
        System.out.println("Duracao: "
                + (System.currentTimeMillis() - inicio) + " ms");
    }

    // Comportamento do produtor da COMINDIR03: 10 leituras, 1 s entre elas
    private void auto() throws InterruptedException {
        for (int i = 1; i <= 10; i++) {
            String sensor = "sensorAuto" + i;

            try {
                enviar(sensor, leituraAleatoria(sensor));
            } catch (IOException e) {
                System.err.println(
                        "Erro ao publicar leitura: " + e.getMessage());
            }

            Thread.sleep(1000);
        }
    }
}
