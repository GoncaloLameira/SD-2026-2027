package rep02;

import java.io.*;
import java.net.Socket;
import java.util.TreeMap;

public class PartitionReplica {

    static final int REPLICA_ID = 1;
    static final int PARTITION = 0;

    static final String TOPIC = "rep02-p0";

    static final int LIMITE_RETIDOS = 10;

    static final String DEFAULT_API =
            "http://127.0.0.1:5001/api/v0";

    public static void main(String[] args) {
        String api = args.length > 0 ? args[0] : DEFAULT_API;

        IpfsPubSub pubSub = new IpfsPubSub(api);

        try {
            RecordFile file =
                    new RecordFile("replica-1-p0.txt");

            long[] ultimo = {file.lastSeq()};

            TreeMap<Long, SensorRecord> pendentes =
                    new TreeMap<>();

            System.out.println("Réplica R1 da partição P0 pronta.");
            System.out.println("Último seq aplicado: " + ultimo[0]);
            System.out.println("A ouvir: " + TOPIC);

            pubSub.subscribe(TOPIC, mensagem -> {
                try {
                    System.out.println("Recebido: " + mensagem);

                    String[] campos = mensagem.split(";", 4);

                    if (campos.length != 4 ||
                            !campos[0].equals("REG")) {

                        System.out.println(
                                "Mensagem inválida: " + mensagem
                        );
                        return;
                    }

                    String leaderAddr = campos[1];
                    int leaderPort =
                            Integer.parseInt(campos[2]);

                    SensorRecord r =
                            SensorRecord.fromLine(campos[3]);

                    long seq = r.getSeq();

                    synchronized (file) {

                        if (seq <= ultimo[0] ||
                                pendentes.containsKey(seq)) {

                            System.out.println(
                                    "Duplicado ignorado: seq=" + seq
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

                            pendentes.put(seq, r);

                            System.out.println(
                                    "Retido: seq=" + seq +
                                            " | esperado=" +
                                            (ultimo[0] + 1)
                            );
                        }

                        if (pendentes.size() > LIMITE_RETIDOS) {

                            long de = ultimo[0] + 1;
                            long ate = pendentes.firstKey() - 1;

                            System.out.println(
                                    "Limite de retidos ultrapassado."
                            );

                            System.out.println(
                                    "A pedir recuperação: " +
                                            de + ".." + ate
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
                            "Erro ao processar REG: " + e
                    );
                }
            });

        } catch (Exception e) {
            System.out.println("Erro: " + e);
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
        ultimo[0] = r.getSeq();

        System.out.println(
                "Aplicado: seq=" + ultimo[0]
        );

        while (pendentes.containsKey(ultimo[0] + 1)) {

            SensorRecord seguinte =
                    pendentes.remove(ultimo[0] + 1);

            file.append(seguinte);
            ultimo[0] = seguinte.getSeq();

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

        try (
                Socket socket =
                        new Socket(leaderAddr, leaderPort);

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
            out.println("PEDIDO " + de + " " + ate);

            String linha;

            while ((linha = in.readLine()) != null) {

                if (linha.equals("FIM")) {
                    break;
                }

                SensorRecord recuperado =
                        SensorRecord.fromLine(linha);

                long seq = recuperado.getSeq();

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
                            "Recuperado retido: seq=" + seq
                    );
                }
            }

            System.out.println("Recuperação terminada.");

        } catch (Exception e) {
            System.out.println(
                    "Erro na recuperação TCP: " + e
            );
        }
    }
}