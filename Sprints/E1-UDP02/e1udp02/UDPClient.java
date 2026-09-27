package e1udp02;
import java.net.*;
import java.io.*;
import java.util.Scanner;

public class UDPClient {

    public static void main(String args[]) {
        DatagramSocket aSocket = null;
        Scanner teclado = new Scanner(System.in);

        try {
            aSocket = new DatagramSocket();

            InetAddress aHost = InetAddress.getByName("localhost");
            int serverPort = 6789;

            System.out.println("Escolha o modo:");
            System.out.println("1 - Automático");
            System.out.println("2 - Manual");
            System.out.print("Modo: ");

            String modo = teclado.nextLine();

            int N = 1;

            while (true){
                System.out.print("Mensagem (ou 'sair'): ");
                String mensagem = teclado.nextLine();

                if (mensagem.equalsIgnoreCase("sair")) {
                    break;
                }

                String mensagemEnviar;

                if (modo.equals("1")) {
                    mensagemEnviar = N + "," + mensagem;
                    N++;
                } else {
                    mensagemEnviar = mensagem;
                }

                byte[] m = mensagemEnviar.getBytes();

                DatagramPacket request = new DatagramPacket(m, m.length, aHost, serverPort);

                aSocket.send(request);

                byte[] buffer = new byte[1000];

                DatagramPacket reply = new DatagramPacket(buffer, buffer.length);

                aSocket.receive(reply);

                String resposta = new String(
                        reply.getData(),
                        reply.getOffset(),
                        reply.getLength()
                );

                if (resposta.startsWith("waitingfor")) {
                    System.out.println("Waitingfor: " + resposta);
                } else {
                    System.out.println("Reply: " + resposta);
                }
            }


        } catch (SocketException e) { System.out.println("Socket: " + e.getMessage());
        } catch (IOException e)     { System.out.println("IO: " + e.getMessage());
        } finally { if (aSocket != null) aSocket.close(); teclado.close();}
    }
}