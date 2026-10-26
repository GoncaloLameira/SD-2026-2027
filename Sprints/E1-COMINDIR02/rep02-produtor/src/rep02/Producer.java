package rep02;

import java.util.Scanner;

public class Producer {

    static final String DEFAULT_API =
            "http://127.0.0.1:5001/api/v0";

    public static void main(String[] args) {

        if (args.length < 1) {
            System.out.println(
                    "Uso: java rep02.Producer <N> [api]"
            );
            return;
        }

        int n = Integer.parseInt(args[0]);

        String api = args.length > 1
                ? args[1]
                : DEFAULT_API;

        IpfsPubSub pubSub = new IpfsPubSub(api);

        Partitioner partitioner = new Partitioner(n);

        Scanner scanner = new Scanner(System.in);

        System.out.println("Producer pronto.");
        System.out.println("Formato: <sensor> <temperatura>");
        System.out.println("Exemplo: S01 23.5");

        while (true) {

            System.out.print("> ");

            String linha = scanner.nextLine().trim();

            if (linha.isEmpty()) {
                continue;
            }

            String[] campos = linha.split("\\s+");


            // =========================================================
            // RAJADA QUENTE
            // =========================================================

            if (campos.length == 3 &&
                    campos[0].equalsIgnoreCase("rajada-quente")) {

                try {

                    int quantidade =
                            Integer.parseInt(campos[1]);

                    int numSensores =
                            Integer.parseInt(campos[2]);

                    int[] contadores =
                            new int[n];

                    long inicio =
                            System.currentTimeMillis();

                    for (int i = 0; i < quantidade; i++) {

                        String sensorId;

                        // 80% das mensagens vão para S01
                        if (Math.random() < 0.8 ||
                                numSensores == 1) {

                            sensorId = "S01";

                        } else {

                            int numeroSensor =
                                    2 +
                                            (int) (
                                                    Math.random() *
                                                            (numSensores - 1)
                                            );

                            sensorId =
                                    String.format(
                                            "S%02d",
                                            numeroSensor
                                    );
                        }

                        double temperatura =
                                15 +
                                        Math.random() * 15;

                        int partitionId =
                                partitioner.of(sensorId);

                        contadores[partitionId]++;

                        String topicIn =
                                Wire.topicIn(partitionId);

                        String mensagem =
                                Wire.in(
                                        sensorId,
                                        temperatura
                                );

                        pubSub.publishWithRetry(
                                topicIn,
                                mensagem
                        );

                        System.out.println(
                                "Enviado para P" +
                                        partitionId +
                                        ": " +
                                        mensagem
                        );
                    }

                    long fim =
                            System.currentTimeMillis();

                    System.out.println(
                            "Primeira publicação: " +
                                    inicio
                    );

                    System.out.println(
                            "Última publicação: " +
                                    fim
                    );

                    System.out.println(
                            "Duração: " +
                                    (fim - inicio) +
                                    " ms"
                    );

                    System.out.println(
                            "Distribuição por partição:"
                    );

                    for (int p = 0; p < n; p++) {

                        System.out.println(
                                "P" +
                                        p +
                                        ": " +
                                        contadores[p] +
                                        " leituras"
                        );
                    }

                } catch (Exception e) {

                    System.out.println(
                            "Erro na rajada-quente: " +
                                    e
                    );
                }

                continue;
            }


            // =========================================================
            // RAJADA NORMAL
            // =========================================================

            if (campos.length == 3 &&
                    campos[0].equalsIgnoreCase("rajada")) {

                try {

                    int quantidade =
                            Integer.parseInt(campos[1]);

                    int numSensores =
                            Integer.parseInt(campos[2]);

                    int[] contadores =
                            new int[n];

                    long inicio =
                            System.currentTimeMillis();

                    for (int i = 0; i < quantidade; i++) {

                        int numeroSensor =
                                1 +
                                        (int) (
                                                Math.random() *
                                                        numSensores
                                        );

                        String sensorId =
                                String.format(
                                        "S%02d",
                                        numeroSensor
                                );

                        double temperatura =
                                15 +
                                        Math.random() * 15;

                        int partitionId =
                                partitioner.of(sensorId);

                        contadores[partitionId]++;

                        String topicIn =
                                Wire.topicIn(partitionId);

                        String mensagem =
                                Wire.in(
                                        sensorId,
                                        temperatura
                                );

                        pubSub.publishWithRetry(
                                topicIn,
                                mensagem
                        );

                        System.out.println(
                                "Enviado para P" +
                                        partitionId +
                                        ": " +
                                        mensagem
                        );
                    }

                    long fim =
                            System.currentTimeMillis();

                    System.out.println(
                            "Primeira publicação: " +
                                    inicio
                    );

                    System.out.println(
                            "Última publicação: " +
                                    fim
                    );

                    System.out.println(
                            "Duração: " +
                                    (fim - inicio) +
                                    " ms"
                    );

                    System.out.println(
                            "Distribuição por partição:"
                    );

                    for (int p = 0; p < n; p++) {

                        System.out.println(
                                "P" +
                                        p +
                                        ": " +
                                        contadores[p] +
                                        " leituras"
                        );
                    }

                } catch (Exception e) {

                    System.out.println(
                            "Erro na rajada: " +
                                    e
                    );
                }

                continue;
            }


            // =========================================================
            // ENVIO MANUAL
            // =========================================================

            if (campos.length != 2) {

                System.out.println(
                        "Formato inválido."
                );

                continue;
            }

            String sensorId =
                    campos[0];

            try {

                double temperatura =
                        Double.parseDouble(
                                campos[1]
                        );

                int partitionId =
                        partitioner.of(sensorId);

                String topicIn =
                        Wire.topicIn(partitionId);

                String mensagem =
                        Wire.in(
                                sensorId,
                                temperatura
                        );

                pubSub.publish(
                        topicIn,
                        mensagem
                );

                System.out.println(
                        "Enviado para P" +
                                partitionId +
                                ": " +
                                mensagem
                );

            } catch (NumberFormatException e) {

                System.out.println(
                        "Temperatura inválida."
                );

            } catch (Exception e) {

                System.out.println(
                        "Erro ao publicar: " +
                                e
                );
            }
        }
    }
}