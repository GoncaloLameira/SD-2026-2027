
package rep03;

import java.io.IOException;

public class TestePubSubIN {

    public static void main(String[] args) {

        IpfsPubSub pubSub = new IpfsPubSub(
                "http://127.0.0.1:5001/api/v0"
        );


        String topico = "rep03-in-p0";
        String mensagem = "IN;sensorTeste;29.5;0;0,0,0";


        try {
            pubSub.publish(topico, mensagem);

            System.out.println("Mensagem enviada: " + mensagem);
            System.out.println("Topico: " + topico);

        } catch (IOException | InterruptedException e) {
            System.out.println(
                    "Erro ao publicar: " + e.getMessage()
            );
        }
    }
}
