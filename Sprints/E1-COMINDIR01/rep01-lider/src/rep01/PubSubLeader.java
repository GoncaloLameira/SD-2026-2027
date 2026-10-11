package rep01;

import java.io.IOException;
import java.util.Random;
import java.util.Scanner;

public class PubSubLeader {

    static final String TOPIC = "rep01-temperaturas";
    static final String DEFAULT_API = "http://127.0.0.1:5001/api/v0";

    public static void main(String[] args) {
        String api = args.length > 0 ? args[0] : DEFAULT_API;

        IpfsPubSub pubSub = new IpfsPubSub(api);

        try (Scanner sc = new Scanner(System.in)) {

            RecordFile file = new RecordFile("lider.txt");
            long seq = file.lastSeq();

            System.out.println("Líder pronto. Formato: <sensor> <temperatura> | 'sair' para terminar");

            while (true) {
                System.out.print("> ");

                if (!sc.hasNextLine()) break;

                String line = sc.nextLine().trim();

                if (line.equalsIgnoreCase("sair")) break;

                if (line.startsWith("rajada ")) {
                    String[] partes = line.split("\\s+");

                    int n;

                    try {
                        n = Integer.parseInt(partes[1]);

                        if (n <= 0) {
                            System.out.println("O número deve ser positivo.");
                            continue;
                        }
                    } catch (NumberFormatException e) {
                        System.out.println("Número inválido.");
                        continue;
                    }

                    Random random = new Random();

                    long primeiroEnvio = 0;
                    long ultimoEnvio = 0;

                    for (int i = 0; i < n; i++) {
                        double temperatura = 15 + random.nextDouble() * 15;

                        SensorRecord r = SensorRecord.now(++seq, "SIM", temperatura);

                        file.append(r);

                        pubSub.publish(TOPIC, r.toLine());

                        long instante = System.currentTimeMillis();

                        if (i == 0) {
                            primeiroEnvio = instante;
                        }

                        ultimoEnvio = instante;
                    }

                    System.out.println("Rajada concluída: " + n + " registos");
                    System.out.println("Primeiro envio: " + primeiroEnvio);
                    System.out.println("Último envio: " + ultimoEnvio);
                    System.out.println("Duração: " + (ultimoEnvio - primeiroEnvio) + " ms");

                    continue;
                }

                String[] p = line.split("\\s+");

                if (p.length != 2) {
                    System.out.println("Formato inválido.");
                    continue;
                }

                double temp;

                try {
                    temp = Double.parseDouble(p[1]);
                } catch (NumberFormatException e) {
                    System.out.println("Temperatura inválida: " + p[1]);
                    continue;
                }

                SensorRecord r = SensorRecord.now(++seq, p[0], temp);

                file.append(r);

                pubSub.publish(TOPIC, r.toLine());

                System.out.println("Registado e enviado: " + r.toLine());
            }

        } catch (IOException | InterruptedException e) {
            System.out.println("Erro: " + e.getMessage());
        }
    }
}