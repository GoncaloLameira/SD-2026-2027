package rep02;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;

public class PartitionLeader {

    static final String DEFAULT_API = "http://127.0.0.1:5001/api/v0";

    public static void main(String[] args) {
        if (args.length < 2) {
            System.out.println(
                    "Uso: java rep02.PartitionLeader <k> <N> [endereço] [api]"
            );
            return;
        }

        int partitionId = Integer.parseInt(args[0]);
        int n = Integer.parseInt(args[1]);

        if (n < 1) {
            System.out.println("Número de partições inválido.");
            return;
        }

        if (partitionId < 0 || partitionId >= n) {
            System.out.println(
                    "Partição inválida: P" + partitionId
            );
            return;
        }

        String leaderAddr = args.length > 2
                ? args[2]
                : "127.0.0.1";

        String api = args.length > 3
                ? args[3]
                : DEFAULT_API;

        int tcpPort = Wire.PORTO_BASE + partitionId;

        String topicIn = Wire.topicIn(partitionId);
        String topicOut = Wire.topicData(partitionId);

        IpfsPubSub pubSub = new IpfsPubSub(api);

        Partitioner partitioner = new Partitioner(n);

        try {
            RecordFile file = new RecordFile("lider-p" + partitionId + ".txt");
            long[] seq = {file.lastSeq()};

            System.out.println("Líder P" + partitionId + " pronto.");

            // Thread que recebe as leituras do Producer através do PubSub
            Thread subscriberThread = new Thread(() -> {
                try {
                    System.out.println("A ouvir PubSub: " + topicIn);

                    pubSub.subscribe(topicIn, mensagem -> {
                        try {
                            System.out.println("Recebido: " + mensagem);

                            String[] campos = mensagem.split(";");

                            if (campos.length != 3 || !campos[0].equals("IN")) {
                                System.out.println("REJEITADO: " + mensagem);
                                return;
                            }

                            String sensorId = campos[1];
                            double temperatura = Double.parseDouble(campos[2]);

                            int particaoEsperada = partitioner.of(sensorId);

                            if (particaoEsperada != partitionId) {
                                System.out.println(
                                        "PARTIÇÃO ERRADA: " + sensorId +
                                                " pertence a p" + particaoEsperada
                                );
                                return;
                            }

                            SensorRecord r;

                            synchronized (file) {
                                r = SensorRecord.now(
                                        ++seq[0],
                                        sensorId,
                                        temperatura
                                );

                                file.append(r);
                            }

                            String mensagemSaida =
                                    Wire.reg(leaderAddr, tcpPort, r);

                            pubSub.publish(topicOut, mensagemSaida);

                            System.out.println("Registado e enviado: " + mensagemSaida);

                        } catch (Exception e) {
                            System.out.println("Erro ao processar mensagem: " + e);
                        }
                    });

                } catch (IOException | InterruptedException e) {
                    System.out.println("Erro no PubSub: " + e);
                }
            });

            subscriberThread.start();

            // Thread principal: servidor TCP para pedidos de recuperação
            try (ServerSocket serverSocket = new ServerSocket(tcpPort)) {

                System.out.println(
                        "Servidor TCP à escuta em " +
                                leaderAddr + ":" + tcpPort
                );

                while (true) {
                    Socket socket = serverSocket.accept();
                    socket.setSoTimeout(Wire.T_TCP_MS);

                    try (
                            BufferedReader in = new BufferedReader(
                                    new InputStreamReader(socket.getInputStream())
                            );

                            PrintWriter out = new PrintWriter(
                                    socket.getOutputStream(), true
                            )
                    ) {
                        String pedido = in.readLine();

                        if (pedido == null) {
                            continue;
                        }

                        System.out.println("Pedido TCP recebido: " + pedido);

                        String[] campos = pedido.trim().split("\\s+");

                        if (campos.length != 3 || !campos[0].equals("PEDIDO")) {
                            System.out.println("Pedido inválido: " + pedido);
                            out.println("ERRO pedido mal formado");
                            continue;
                        }

                        long from;
                        long to;

                        try {
                            from = Long.parseLong(campos[1]);
                            to = Long.parseLong(campos[2]);
                        } catch (NumberFormatException e) {
                            System.out.println("Pedido inválido: " + pedido);
                            out.println("ERRO valores invalidos");
                            continue;
                        }

                        if (from > to) {
                            System.out.println("Intervalo inválido: " + from + ".." + to);
                            out.println("ERRO intervalo invalido");
                            continue;
                        }

                        long ultimoSeq = seq[0];

                        if (to > ultimoSeq) {
                            to = ultimoSeq;
                        }

                        List<SensorRecord> records = file.range(from, to);

                        System.out.println(
                                "RECUPERAÇÃO: " +
                                        from + ".." + to +
                                        " para " + socket.getRemoteSocketAddress() +
                                        " (" + records.size() + " registos)"
                        );

                        for (SensorRecord r : records) {
                            out.println(r.toLine());
                        }

                        out.println("FIM");

                    } catch (Exception e) {
                        System.out.println("Erro na recuperação TCP: " + e);

                    } finally {
                        try {
                            socket.close();
                        } catch (IOException ignored) {
                        }
                    }
                }
            }

        } catch (IOException e) {
            System.out.println("Erro: " + e);
            e.printStackTrace();
        }
    }
}