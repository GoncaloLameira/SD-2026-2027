package rep02;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;

public class PartitionLeader {

    static final int PARTITION = 0;

    static final String TOPIC_IN = "rep02-in-p0";
    static final String TOPIC_OUT = "rep02-p0";

    static final String LEADER_ADDR = "127.0.0.1";
    static final int DEFAULT_TCP_PORT = 7000;

    static final String DEFAULT_API = "http://127.0.0.1:5001/api/v0";

    public static void main(String[] args) {
        String api = args.length > 0 ? args[0] : DEFAULT_API;
        int tcpPort = args.length > 1
                ? Integer.parseInt(args[1])
                : DEFAULT_TCP_PORT;

        IpfsPubSub pubSub = new IpfsPubSub(api);

        try {
            RecordFile file = new RecordFile("lider-p0.txt");
            long[] seq = {file.lastSeq()};

            System.out.println("Líder P0 pronto.");

            // Thread que recebe as leituras do Producer através do PubSub
            Thread subscriberThread = new Thread(() -> {
                try {
                    System.out.println("A ouvir PubSub: " + TOPIC_IN);

                    pubSub.subscribe(TOPIC_IN, mensagem -> {
                        try {
                            System.out.println("Recebido: " + mensagem);

                            String[] campos = mensagem.split(";");

                            if (campos.length != 3 || !campos[0].equals("IN")) {
                                System.out.println("Mensagem inválida: " + mensagem);
                                return;
                            }

                            String sensorId = campos[1];
                            double temperatura = Double.parseDouble(campos[2]);

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
                                    "REG;" + LEADER_ADDR + ";" + tcpPort + ";" + r.toLine();

                            pubSub.publish(TOPIC_OUT, mensagemSaida);

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
                                LEADER_ADDR + ":" + tcpPort
                );

                while (true) {
                    Socket socket = serverSocket.accept();

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
                            out.println("FIM");
                            continue;
                        }

                        long from = Long.parseLong(campos[1]);
                        long to = Long.parseLong(campos[2]);

                        List<SensorRecord> records = file.range(from, to);

                        System.out.println(
                                "Recuperação: " +
                                        from + ".." + to +
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