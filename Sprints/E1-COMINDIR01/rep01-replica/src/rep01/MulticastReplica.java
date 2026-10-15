package rep01;

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.TreeMap;

public class MulticastReplica {
    static final String GROUP = "230.0.0.1";
    static final int PORT = 6789;

    public static void main(String[] args) {
        String id = args.length > 0 ? args[0] : "1";
        RecordFile file = new RecordFile("replica-" + id + ".txt");

        TreeMap<Long, SensorRecord> pendentes = new TreeMap<>();

        long recebidos = 0;
        long escritos = 0;
        long primeiroRecebido = 0;
        long ultimoRecebido = 0;

        boolean estatisticasMostradas = false;

        try (MulticastSocket socket = new MulticastSocket(PORT)) {
            InetAddress group = InetAddress.getByName(GROUP);
            socket.joinGroup(group);
            socket.setSoTimeout(1000);

            long ultimo = file.lastSeq();

            System.out.println("Réplica " + id + " à escuta em " + GROUP + ":" + PORT);

            byte[] buffer = new byte[1000];

            while (true) {
                DatagramPacket p = new DatagramPacket(buffer, buffer.length);

                try {
                    socket.receive(p);
                } catch (SocketTimeoutException e) {
                    if (recebidos > 0 && !estatisticasMostradas) {
                        System.out.println();
                        System.out.println("--- Estatísticas ---");
                        System.out.println("Primeiro recebido: " + primeiroRecebido);
                        System.out.println("Último recebido: " + ultimoRecebido);
                        System.out.println("Duração da receção: " + (ultimoRecebido - primeiroRecebido) + " ms");
                        System.out.println("Recebidos: " + recebidos);
                        System.out.println("Escritos: " + escritos);
                        System.out.println("Retidos em memória: " + pendentes.size());
                        System.out.println("--------------------");
                        System.out.println();
                        estatisticasMostradas = true;
                    }

                    continue;
                }

                String line = new String(
                        p.getData(), 0, p.getLength(), StandardCharsets.UTF_8
                );

                try {

                    SensorRecord r = SensorRecord.fromLine(line);

                    long agora = System.currentTimeMillis();

                    recebidos++;

                    if (recebidos == 1) {
                        primeiroRecebido = agora;
                    }

                    ultimoRecebido = agora;

                    long seq = r.getSeq();
                    if (seq <= ultimo || pendentes.containsKey(seq)) {
                        System.out.println("DUPLICADO: " + seq);
                    }
                    else if (seq == ultimo + 1) {

                        file.append(r);
                        escritos++;
                        ultimo = seq;

                        System.out.println("Aplicado: " + r.toLine());

                        while (pendentes.containsKey(ultimo + 1)) {
                            SensorRecord seguinte = pendentes.remove(ultimo + 1);

                            file.append(seguinte);
                            escritos++;

                            ultimo = seguinte.getSeq();

                            System.out.println("Aplicado: " + seguinte.toLine());
                        }
                    }
                    else {
                        pendentes.put(seq, r);

                        System.out.println(
                                "EM ESPERA: " + seq +
                                        " (falta o registo " + (ultimo + 1) + ")"
                        );
                    }

                } catch (IllegalArgumentException e) {
                    System.out.println("REJEITADO: " + line);
                }
            }

        } catch (IOException e) {
            System.out.println("IO: " + e.getMessage());
        }
    }
}
