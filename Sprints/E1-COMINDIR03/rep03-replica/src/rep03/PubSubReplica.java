package rep03;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

public class PubSubReplica {
    static final String DEFAULT_API = "http://127.0.0.1:5001/api/v0";
    static final String DEFAULT_HOST = "127.0.0.1";
    static final int PARTITIONS = 3;
    static final int TIMEOUT_MS = 5000;

    public static void main(String[] args) {
        String api = args.length > 0 ? args[0] : DEFAULT_API;
        IpfsPubSub pubSub = new IpfsPubSub(api);
        RecordFile ficheiro = new RecordFile("rep03-replica-registos.txt");

        System.out.println("Replica pronta. A subscrever " + PARTITIONS + " particoes.");
        for (int p = 0; p < PARTITIONS; p++) {
            final int particaoTopico = p;
            final String topico = "rep03-p" + p;
            Thread thread = new Thread(() -> {
                try {
                    System.out.println("A subscrever: " + topico);
                    pubSub.subscribe(topico, mensagem ->
                            processarMensagem(mensagem, particaoTopico, ficheiro));
                } catch (Exception e) {
                    System.out.println("Erro na subscricao " + topico + ": " + e.getMessage());
                }
            }, "replica-p" + p);
            thread.start();
        }
    }

    private static void processarMensagem(String mensagem, int particaoTopico, RecordFile ficheiro) {
        System.out.println("Mensagem recebida em p" + particaoTopico + ": " + mensagem);
        try {
            String host = DEFAULT_HOST;
            int porta = 7000 + particaoTopico;
            String reg;

            if (mensagem.startsWith("REG;")) {
                String[] campos = mensagem.split(";", 4);
                if (campos.length != 4 || campos[1].isBlank()) {
                    System.out.println("REG mal formado.");
                    return;
                }
                host = campos[1].trim();
                porta = Integer.parseInt(campos[2].trim());
                reg = campos[3].trim();
            } else if (mensagem.startsWith("REG |")) {
                reg = mensagem.trim();
            } else {
                System.out.println("Mensagem ignorada: nao e REG.");
                return;
            }

            // Converte o formato novo para o formato antigo
// utilizado pelo RecordFile da replica.

            // Converte os registos recebidos para o formato
            // utilizado pelo RecordFile da replica.
            if (!reg.startsWith("REG |")) {

                String[] dados = reg.split(";", -1);

                if (dados.length != 6) {
                    System.out.println("Conteudo REG invalido: " + reg);
                    return;
                }

                // Formato antigo:
                // particao;seq;sensor;temperatura;L;V
                if (dados[1].trim().matches("\\d+")) {

                    reg = "REG | p=" + dados[0]
                            + " | seq=" + dados[1]
                            + " | sensor=" + dados[2]
                            + " | temp=" + dados[3]
                            + " | L=" + dados[4]
                            + " | V=[" + dados[5].replace(",", ", ") + "]";

                } else {

                    // Formato novo:
                    // seq;sensor;temperatura;instante;L;V
                    // A particao vem do topico recebido.

                    reg = "REG | p=" + particaoTopico
                            + " | seq=" + dados[0]
                            + " | sensor=" + dados[1]
                            + " | temp=" + dados[2]
                            + " | instante=" + dados[3]
                            + " | L=" + dados[4]
                            + " | V=[" + dados[5].replace(",", ", ") + "]";
                }
            }

            int particao = -1;
            long sequencia = -1;
            for (String campo : reg.split("\\|")) {
                String valor = campo.trim();
                if (valor.startsWith("p=")) particao = Integer.parseInt(valor.substring(2).trim());
                if (valor.startsWith("seq=")) sequencia = Long.parseLong(valor.substring(4).trim());
            }
            if (particao != particaoTopico || sequencia < 1 || porta < 1 || porta > 65535) {
                System.out.println("Particao, sequencia ou porta invalida.");
                return;
            }

            synchronized (ficheiro) {
                if (ficheiro.contains(particao, sequencia)) {
                    System.out.println("Registo duplicado. Ignorado.");
                    return;
                }

                long primeiraEmFalta = ficheiro.firstMissing(particao, sequencia - 1);
                if (primeiraEmFalta < sequencia) {
                    System.out.println("ATENCAO: registos em falta na particao " + particao);
                    long inicio = -1;
                    for (long s = primeiraEmFalta; s < sequencia; s++) {
                        if (!ficheiro.contains(particao, s)) {
                            if (inicio == -1) inicio = s;
                        } else if (inicio != -1) {
                            if (!recuperarRegistos(host, porta, particao, inicio, s - 1, ficheiro)) {
                                System.out.println("Recuperacao incompleta. REG atual nao guardado.");
                                return;
                            }
                            inicio = -1;
                        }
                    }
                    if (inicio != -1 && !recuperarRegistos(host, porta, particao, inicio,
                            sequencia - 1, ficheiro)) {
                        System.out.println("Recuperacao incompleta. REG atual nao guardado.");
                        return;
                    }
                }
                ficheiro.append(reg);
                System.out.println("Sequencia aceite: " + sequencia);
                System.out.println("Registo guardado no ficheiro.");
            }
        } catch (IOException | IllegalArgumentException e) {
            System.out.println("Erro ao processar REG: " + e.getMessage());
        }
    }

    private static boolean recuperarRegistos(String host, int porta, int particao,
                                             long from, long to, RecordFile ficheiro) {
        System.out.println("A pedir recuperacao TCP a " + host + ":" + porta
                + " - PEDIDO " + from + " " + to);
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(host, porta), TIMEOUT_MS);
            socket.setSoTimeout(TIMEOUT_MS);
            BufferedReader in = new BufferedReader(new InputStreamReader(
                    socket.getInputStream(), StandardCharsets.UTF_8));
            PrintWriter out = new PrintWriter(new OutputStreamWriter(
                    socket.getOutputStream(), StandardCharsets.UTF_8), true);
            out.println("PEDIDO " + from + " " + to);
            long esperado = from;
            int recuperados = 0;
            boolean recebeuFim = false;
            String linha;
            while ((linha = in.readLine()) != null) {
                if (linha.equals("FIM")) {
                    recebeuFim = true;
                    break;
                }
                if (linha.startsWith("ERRO")) {
                    System.out.println("Erro do lider: " + linha);
                    return false;
                }
                String[] campos = linha.split(";", -1);
                if (campos.length != 6) {
                    System.out.println("Registo TCP invalido: " + linha);
                    return false;
                }

                int p;
                long seq;
                String reg;

// Formato antigo: p;seq;sensor;temp;L;V
                if (campos[1].trim().matches("\\d+")) {

                    p = Integer.parseInt(campos[0].trim());
                    seq = Long.parseLong(campos[1].trim());

                    reg = "REG | p=" + p
                            + " | seq=" + seq
                            + " | sensor=" + campos[2]
                            + " | temp=" + campos[3]
                            + " | L=" + campos[4]
                            + " | V=[" + campos[5].replace(",", ", ") + "]";

                } else {

                    // Formato novo: seq;sensor;temp;instante;L;V
                    p = particao;
                    seq = Long.parseLong(campos[0].trim());

                    reg = "REG | p=" + p
                            + " | seq=" + seq
                            + " | sensor=" + campos[1]
                            + " | temp=" + campos[2]
                            + " | instante=" + campos[3]
                            + " | L=" + campos[4]
                            + " | V=[" + campos[5].replace(",", ", ") + "]";
                }

                if (p != particao || seq != esperado || seq > to) {
                    System.out.println("Sequencia inesperada na recuperacao: " + seq);
                    return false;
                }

                if (!ficheiro.contains(p, seq)) {
                    ficheiro.append(reg);
                    recuperados++;
                    System.out.println("Registo recuperado e guardado: seq=" + seq);
                }
                esperado++;
            }
            boolean completo = recebeuFim && esperado == to + 1;
            System.out.println(completo ? "Recuperacao TCP concluida: " + recuperados + " registos."
                    : "Recuperacao TCP incompleta.");
            return completo;
        } catch (IOException | IllegalArgumentException e) {
            System.out.println("Erro na recuperacao TCP: " + e.getMessage());
            return false;
        }
    }
}
