package e1udp02;
import java.net.*;
import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;

public class UDPServer {

    // Mensagens que já foram entregues por ordem
    static ArrayList<String> deliveredMessages = new ArrayList<>();

    // Mensagens recebidas fora de ordem
    // Número da mensagem -> conteúdo da mensagem
    static HashMap<Integer, String> temporaryMessages = new HashMap<>();

    /**
     * Processes delivered messages
     * @return the last message processed in order
     */
    public static int processDeliveredMessages(
            int nLastMessageInOrder,
            int nCurrentMessage,
            String currentMessage) {

        // A mensagem que chegou é exatamente a próxima esperada
        if (nCurrentMessage == nLastMessageInOrder + 1) {

            deliveredMessages.add(currentMessage);
            System.out.println("Entregue: " + currentMessage);

            nLastMessageInOrder = nCurrentMessage;

            // Verificar se existem mensagens consecutivas
            // guardadas na estrutura temporária
            while (temporaryMessages.containsKey(nLastMessageInOrder + 1)) {

                int nextMessage = nLastMessageInOrder + 1;

                String message = temporaryMessages.remove(nextMessage);

                deliveredMessages.add(message);

                System.out.println("Entregue da estrutura temporária: " + message);

                nLastMessageInOrder = nextMessage;
            }

        } else {

            // A mensagem chegou fora de ordem.
            temporaryMessages.put(nCurrentMessage, currentMessage);
        }

        return nLastMessageInOrder;
    }


    public static void main(String args[]) {

        DatagramSocket aSocket = null;

        // Número da última mensagem entregue por ordem
        int L = 0;

        try {

            aSocket = new DatagramSocket(6789);

            byte[] buffer = new byte[1000];

            while (true) {

                DatagramPacket request =
                        new DatagramPacket(buffer, buffer.length);

                aSocket.receive(request);

                String mensagem = new String(
                        request.getData(),
                        request.getOffset(),
                        request.getLength()
                );

                String[] partes = mensagem.split(",", 2);

                String resposta;

                if (partes.length != 2) {

                    resposta = "Mensagem inválida";

                } else {

                    try {

                        int N = Integer.parseInt(partes[0].trim());

                        int oldL = L;

                        L = processDeliveredMessages(
                                L,
                                N,
                                mensagem
                        );

                        // Se L não mudou, a mensagem recebida
                        // não pôde ser entregue
                        if (L == oldL) {

                            resposta = "waitingfor," + (L + 1);

                        } else {

                            // Mantém o comportamento de echo
                            resposta = mensagem;
                        }

                        System.out.println();
                        System.out.println("Mensagem recebida: " + mensagem);
                        System.out.println("L = " + L);
                        System.out.println(
                                "Estrutura temporária = " + temporaryMessages
                        );
                        System.out.println(
                                "Lista de receção = " + deliveredMessages
                        );
                        System.out.println("-----------------------------");

                    } catch (NumberFormatException e) {

                        resposta = "Mensagem inválida";
                    }
                }

                byte[] dadosResposta = resposta.getBytes();

                DatagramPacket reply =
                        new DatagramPacket(
                                dadosResposta,
                                dadosResposta.length,
                                request.getAddress(),
                                request.getPort()
                        );

                aSocket.send(reply);
            }

        } catch (SocketException e) {

            System.out.println("Socket: " + e.getMessage());

        } catch (IOException e) {

            System.out.println("IO: " + e.getMessage());

        } finally {

            if (aSocket != null)
                aSocket.close();
        }
    }
}