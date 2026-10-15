package rep01;

import java.io.IOException;
import java.util.TreeMap;

public class PubSubReplica {

    static final String TOPIC = "rep01-temperaturas";

    public static void main(String[] args) {
        String id = args.length > 0 ? args[0] : "1";
        String port = args.length > 1 ? args[1] : "5001";

        String api = "http://127.0.0.1:" + port + "/api/v0";

        RecordFile file = new RecordFile("replica-" + id + ".txt");
        TreeMap<Long, SensorRecord> pendentes = new TreeMap<>();

        long[] recebidos = {0};
        long[] escritos = {0};
        long[] primeiroRecebido = {0};
        long[] ultimoRecebido = {0};

        try {
            long[] ultimo = {file.lastSeq()};

            IpfsPubSub pubSub = new IpfsPubSub(api);

            System.out.println(
                    "Réplica " + id + " à escuta no tópico " + TOPIC +
                            " através da API " + api
            );

            pubSub.subscribe(TOPIC, line -> {
                try {
                    SensorRecord r = SensorRecord.fromLine(line);

                    long agora = System.currentTimeMillis();

                    recebidos[0]++;

                    if (recebidos[0] == 1) {
                        primeiroRecebido[0] = agora;
                    }

                    ultimoRecebido[0] = agora;

                    long seq = r.getSeq();

                    if (seq <= ultimo[0] || pendentes.containsKey(seq)) {
                        System.out.println("DUPLICADO: " + seq);
                    }
                    else if (seq == ultimo[0] + 1) {
                        file.append(r);
                        escritos[0]++;
                        ultimo[0] = seq;

                        System.out.println("Aplicado: " + r.toLine());

                        while (pendentes.containsKey(ultimo[0] + 1)) {
                            SensorRecord seguinte = pendentes.remove(ultimo[0] + 1);

                            file.append(seguinte);
                            escritos[0]++;
                            ultimo[0] = seguinte.getSeq();

                            System.out.println("Aplicado: " + seguinte.toLine());
                        }
                    }
                    else {
                        pendentes.put(seq, r);

                        System.out.println(
                                "EM ESPERA: " + seq +
                                        " (falta o registo " + (ultimo[0] + 1) + ")"
                        );
                    }

                    if (recebidos[0] % 1000 == 0) {
                        System.out.println();
                        System.out.println("--- Estatísticas ---");
                        System.out.println("Primeiro recebido: " + primeiroRecebido[0]);
                        System.out.println("Último recebido: " + ultimoRecebido[0]);
                        System.out.println("Duração da receção: " +
                                (ultimoRecebido[0] - primeiroRecebido[0]) + " ms");
                        System.out.println("Recebidos: " + recebidos[0]);
                        System.out.println("Escritos: " + escritos[0]);
                        System.out.println("Retidos em memória: " + pendentes.size());
                        System.out.println("--------------------");
                        System.out.println();

                        recebidos[0] = 0;
                        escritos[0] = 0;
                        primeiroRecebido[0] = 0;
                        ultimoRecebido[0] = 0;
                    }

                } catch (IllegalArgumentException e) {
                    System.out.println("REJEITADO: " + line);
                } catch (IOException e) {
                    System.out.println("IO: " + e.getMessage());
                }
            });

        } catch (IOException | InterruptedException e) {
            System.out.println("Erro: " + e.getMessage());
        }
    }
}