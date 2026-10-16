package rep02;

import java.util.Scanner;

public class Producer {

    static final String TOPIC_IN = "rep02-in-p0";

    static final String DEFAULT_API =
            "http://127.0.0.1:5001/api/v0";

    public static void main(String[] args) {
        String api = args.length > 0 ? args[0] : DEFAULT_API;

        IpfsPubSub pubSub = new IpfsPubSub(api);

        Scanner scanner = new Scanner(System.in);

        System.out.println("Producer pronto.");
        System.out.println("Formato: <sensor> <temperatura>");
        System.out.println("Exemplo: S01 23.5");

        while (true) {
            System.out.print("> ");

            String linha = scanner.nextLine().trim();

            if (linha.isEmpty()) {
                continue;
            }

            String[] campos = linha.split("\\s+");

            if (campos.length != 2) {
                System.out.println("Formato inválido.");
                continue;
            }

            String sensorId = campos[0];

            try {
                double temperatura = Double.parseDouble(campos[1]);

                String mensagem =
                        "IN;" + sensorId + ";" + temperatura;

                pubSub.publish(TOPIC_IN, mensagem);

                System.out.println("Enviado: " + mensagem);

            } catch (NumberFormatException e) {
                System.out.println("Temperatura inválida.");

            } catch (Exception e) {
                System.out.println("Erro ao publicar: " + e);
            }
        }
    }
}