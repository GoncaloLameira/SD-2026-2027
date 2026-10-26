
package rep03;

import java.util.Locale;
import java.util.Random;

public class PubSubProdutor {

    private static final String DEFAULT_API =
            "http://127.0.0.1:5001/api/v0";

    // FALHA DELIBERADA 5: produtor usa 4 particoes,
// enquanto os lideres e a replica usam apenas 3.
    private static final int PARTITIONS = 3;

    public static void main(String[] args) {

        IpfsPubSub pubSub = new IpfsPubSub(DEFAULT_API);
        Partitioner partitioner = new Partitioner(PARTITIONS);
        Random random = new Random();

        System.out.println("Produtor iniciado.");
        System.out.println("Particoes: " + PARTITIONS);

        for (int i = 1; i <= 10; i++) {

            String sensor = "sensorAuto" + i;
            double temperatura = 15 + random.nextDouble() * 25;

            String mensagem = String.format(
                    Locale.US,
                    "IN;%s;%.2f;0;0,0,0",
                    sensor,
                    temperatura
            );

            int particao = partitioner.of(sensor);
            String topico = "rep03-in-p" + particao;

            try {
                pubSub.publish(topico, mensagem);

                System.out.println(
                        "Publicado: " + mensagem
                                + " | particao=" + particao
                                + " | topico=" + topico
                );

                Thread.sleep(1000);

            } catch (Exception e) {
                System.err.println(
                        "Erro ao publicar leitura: " + e.getMessage()
                );
            }
        }

        System.out.println("Produtor terminado.");
    }
}
