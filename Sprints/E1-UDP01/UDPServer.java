import java.net.*;
import java.io.*;

public class UDPServer {

    public static void main(String args[]) {
        DatagramSocket aSocket = null;

        int L = 0;

        try {
            aSocket = new DatagramSocket(6789);
            byte[] buffer = new byte[1000];

            while (true) {
                DatagramPacket request = new DatagramPacket(buffer, buffer.length);
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
                        int N = Integer.parseInt(partes[0]);

                        if(N == L + 1) {
                            L = N;

                            resposta = mensagem;
                        } else {
                            resposta = "waitingfor," + (L+1);
                        }
                    } catch (NumberFormatException e) {
                        resposta = "Mensagem inválida";
                    }
                }

                byte[] dadosResposta = resposta.getBytes();

                DatagramPacket reply = new DatagramPacket(
                        dadosResposta,
                        dadosResposta.length,
                        request.getAddress(),
                        request.getPort()
                );

                aSocket.send(reply);
            }
        } catch (SocketException e) { System.out.println("Socket: " + e.getMessage());
        } catch (IOException e)     { System.out.println("IO: " + e.getMessage());
        } finally { if (aSocket != null) aSocket.close(); }
    }
}