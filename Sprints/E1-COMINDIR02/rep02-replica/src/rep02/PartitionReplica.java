package rep02;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.TreeMap;

public class PartitionReplica {

    static final String DEFAULT_API =
            "http://127.0.0.1:5001/api/v0";

    public static void main(String[] args) {

        if (args.length < 2) {
            System.out.println(
                    "Uso: java rep02.PartitionReplica <replicaId> <particoes> [api]"
            );
            return;
        }

        int replicaId = Integer.parseInt(args[0]);
        String[] partitionArgs = args[1].split(",");

        double perda = Double.parseDouble(
                System.getProperty("perda", "0")
        );

        long descartar = Long.parseLong(
                System.getProperty("descartar", "-1")
        );

        if (replicaId < 1 || replicaId > 3) {
            System.out.println("Réplica inválida: " + replicaId);
            return;
        }

        /*
         * Validação das atribuições.
         *
         * Topologia normal N=3:
         *   P0 -> R1, R3
         *   P1 -> R1, R2
         *   P2 -> R2, R3
         *
         * O cenário experimental N=1 utiliza apenas P0
         * e coloca P0 nas três réplicas, pelo que R2/P0
         * também é aceite.
         */
        for (String partitionArg : partitionArgs) {

            int partitionId = Integer.parseInt(partitionArg);

            if (partitionId < 0 || partitionId >= 3) {
                System.out.println(
                        "Partição inválida: " + partitionId
                );
                return;
            }

            boolean atribuicaoValida =
                    (partitionId == 0 &&
                            (replicaId == 1 ||
                                    replicaId == 2 ||
                                    replicaId == 3))
                            ||
                            (partitionId == 1 &&
                                    (replicaId == 1 ||
                                            replicaId == 2))
                            ||
                            (partitionId == 2 &&
                                    (replicaId == 2 ||
                                            replicaId == 3));

            if (!atribuicaoValida) {
                System.out.println(
                        "A partição P" + partitionId +
                                " não está atribuída à réplica R" +
                                replicaId + "."
                );
                return;
            }
        }

        String api = args.length > 2
                ? args[2]
                : DEFAULT_API;

        // Uma thread independente para cada partição
        for (String partitionArg : partitionArgs) {

            int partitionId = Integer.parseInt(partitionArg);

            Thread thread = new Thread(() -> {
                executarParticao(
                        replicaId,
                        partitionId,
                        api,
                        perda,
                        descartar
                );
            });

            thread.start();
        }
    }

    private static void executarParticao(
            int replicaId,
            int partitionId,
            String api,
            double perda,
            long descartar
    ) {

        String topic = Wire.topicData(partitionId);

        IpfsPubSub pubSub = new IpfsPubSub(api);

        try {

            RecordFile file =
                    new RecordFile(
                            "replica-" +
                                    replicaId +
                                    "-p" +
                                    partitionId +
                                    ".txt"
                    );

            long[] ultimo = {file.lastSeq()};

            TreeMap<Long, SensorRecord> pendentes =
                    new TreeMap<>();

            System.out.println(
                    "Réplica R" + replicaId +
                            " da partição P" +
                            partitionId +
                            " pronta."
            );

            System.out.println(
                    "Último seq aplicado: " + ultimo[0]
            );

            System.out.println(
                    "A ouvir: " + topic
            );

            boolean[] descarteFeito = {false};

            pubSub.subscribe(topic, mensagem -> {

                try {

                    System.out.println(
                            "Recebido: " + mensagem
                    );

                    String[] campos =
                            Wire.regParts(mensagem);

                    String leaderAddr =
                            campos[0];

                    int leaderPort =
                            Integer.parseInt(campos[1]);

                    SensorRecord r =
                            SensorRecord.fromLine(campos[2]);

                    long seq =
                            r.getSeq();

                    /*
                     * Simulação determinística:
                     * descarta apenas a primeira receção
                     * da sequência indicada.
                     */
                    if (seq == descartar && !descarteFeito[0]) {

                        descarteFeito[0] = true;

                        System.out.println(
                                "DESCARTADO (simulação): " + seq
                        );

                        return;
                    }

                    /*
                     * Simulação probabilística de perda.
                     * Aplica-se apenas aos REG recebidos
                     * através do PubSub.
                     */
                    if (Math.random() < perda) {

                        System.out.println(
                                "DESCARTADO (simulação): " + seq
                        );

                        return;
                    }

                    synchronized (file) {

                        if (seq <= ultimo[0] ||
                                pendentes.containsKey(seq)) {

                            System.out.println(
                                    "Duplicado ignorado: seq=" +
                                            seq
                            );

                            return;
                        }

                        if (seq == ultimo[0] + 1) {

                            aplicar(
                                    r,
                                    file,
                                    ultimo,
                                    pendentes
                            );

                        } else {

                            pendentes.put(
                                    seq,
                                    r
                            );

                            System.out.println(
                                    "Retido: seq=" +
                                            seq +
                                            " | esperado=" +
                                            (ultimo[0] + 1)
                            );
                        }

                        /*
                         * A recuperação só é iniciada
                         * quando existem MAIS de 10
                         * registos retidos.
                         */
                        if (pendentes.size() >
                                Wire.LIMITE_RETIDOS) {

                            long de =
                                    ultimo[0] + 1;

                            long ate =
                                    pendentes.firstKey() - 1;

                            System.out.println(
                                    "Limite de retidos ultrapassado."
                            );

                            System.out.println(
                                    "RECUPERAÇÃO: " +
                                            de + ".." + ate +
                                            " pedida a " +
                                            leaderAddr + ":" +
                                            leaderPort
                            );

                            recuperar(
                                    leaderAddr,
                                    leaderPort,
                                    de,
                                    ate,
                                    file,
                                    ultimo,
                                    pendentes
                            );
                        }
                    }

                } catch (Exception e) {

                    System.out.println(
                            "Erro ao processar REG: " +
                                    e
                    );
                }
            });

        } catch (Exception e) {

            System.out.println(
                    "Erro: " + e
            );

            e.printStackTrace();
        }
    }

    private static void aplicar(
            SensorRecord r,
            RecordFile file,
            long[] ultimo,
            TreeMap<Long, SensorRecord> pendentes
    ) throws IOException {

        file.append(r);

        ultimo[0] =
                r.getSeq();

        System.out.println(
                "Aplicado: seq=" +
                        ultimo[0]
        );

        /*
         * Depois de aplicar um registo,
         * aplica todos os pendentes que
         * tenham ficado consecutivos.
         */
        while (
                pendentes.containsKey(
                        ultimo[0] + 1
                )
        ) {

            SensorRecord seguinte =
                    pendentes.remove(
                            ultimo[0] + 1
                    );

            file.append(
                    seguinte
            );

            ultimo[0] =
                    seguinte.getSeq();

            System.out.println(
                    "Aplicado pendente: seq=" +
                            ultimo[0]
            );
        }
    }

    private static void recuperar(
            String leaderAddr,
            int leaderPort,
            long de,
            long ate,
            RecordFile file,
            long[] ultimo,
            TreeMap<Long, SensorRecord> pendentes
    ) {

        Socket socket = new Socket();

        try {

            // Timeout máximo para estabelecer ligação ao Leader
            socket.connect(
                    new InetSocketAddress(
                            leaderAddr,
                            leaderPort
                    ),
                    Wire.T_TCP_MS
            );

            // Timeout máximo à espera de dados do Leader
            socket.setSoTimeout(
                    Wire.T_TCP_MS
            );

            try (
                    BufferedReader in =
                            new BufferedReader(
                                    new InputStreamReader(
                                            socket.getInputStream()
                                    )
                            );

                    PrintWriter out =
                            new PrintWriter(
                                    socket.getOutputStream(),
                                    true
                            )
            ) {

                out.println(
                        "PEDIDO " +
                                de +
                                " " +
                                ate
                );

                String linha;

                while (
                        (linha = in.readLine()) != null
                ) {

                    if (linha.equals("FIM")) {
                        break;
                    }

                    if (linha.startsWith("ERRO")) {

                        System.out.println(
                                "Erro recebido do Leader: " +
                                        linha
                        );

                        break;
                    }

                    SensorRecord recuperado =
                            SensorRecord.fromLine(
                                    linha
                            );

                    long seq =
                            recuperado.getSeq();

                    System.out.println(
                            "RECUPERADO: " + seq
                    );

                    /*
                     * As simulações -Dperda e -Ddescartar
                     * NÃO são aplicadas aos registos
                     * recuperados por TCP.
                     */
                    if (seq <= ultimo[0] ||
                            pendentes.containsKey(seq)) {

                        System.out.println(
                                "Recuperado duplicado ignorado: seq=" +
                                        seq
                        );

                        continue;
                    }

                    if (seq == ultimo[0] + 1) {

                        aplicar(
                                recuperado,
                                file,
                                ultimo,
                                pendentes
                        );

                    } else {

                        pendentes.put(
                                seq,
                                recuperado
                        );

                        System.out.println(
                                "Recuperado retido: seq=" +
                                        seq
                        );
                    }
                }

                System.out.println(
                        "Recuperação terminada."
                );
            }

        } catch (Exception e) {

            System.out.println(
                    "Erro na recuperação TCP: " +
                            e
            );

        } finally {

            try {
                socket.close();
            } catch (IOException ignored) {
            }
        }
    }
}