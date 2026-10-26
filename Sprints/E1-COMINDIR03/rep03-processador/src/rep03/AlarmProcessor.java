
package rep03;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class AlarmProcessor {

    private static final String DEFAULT_API = "http://127.0.0.1:5001/api/v0";
    private static final double LIMIAR_ALARME = 28.0;

    private final IpfsPubSub ipfs;
    private final Partitioner partitioner;
    private final long[] vetor;
    private final Set<Long>[] recebidos;

    private long lamport = 0;

    @SuppressWarnings("unchecked")
    public AlarmProcessor(int n, String api) {
        ipfs = new IpfsPubSub(api);
        partitioner = new Partitioner(n);
        vetor = new long[n];

        recebidos = (Set<Long>[]) new Set[n];
        for (int i = 0; i < n; i++) {
            recebidos[i] = new HashSet<>();
        }
    }

    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("Uso: AlarmProcessor <N> [api]");
            return;
        }

        int n = Integer.parseInt(args[0]);
        String api = args.length > 1 ? args[1] : DEFAULT_API;

        AlarmProcessor processador = new AlarmProcessor(n, api);
        processador.iniciar();
    }

    private void iniciar() {
        for (int k = 0; k < partitioner.count(); k++) {
            final int particao = k;

            Thread thread = new Thread(() -> {
                while (!Thread.currentThread().isInterrupted()) {
                    try {
                        String topico = Wire.topicData(particao);

                        System.out.println("A subscrever: " + topico);

                        ipfs.subscribe(topico,
                                mensagem -> processar(mensagem, particao));

                    } catch (Exception e) {
                        System.err.println("Erro na particao "
                                + particao + ": " + e.getMessage());

                        try {
                            Thread.sleep(2000);
                        } catch (InterruptedException ex) {
                            Thread.currentThread().interrupt();
                        }
                    }
                }
            }, "AlarmProcessor-p" + k);

            thread.start();
        }

        System.out.println("Processador de alarmes pronto.");
    }

    private void processar(String mensagem, int particao) {
        try {
            String[] campos = mensagem.trim().split(";", -1);

            if (campos.length != 9 || !campos[0].equals("REG")) {
                return;
            }


            long sequencia;
            String sensor;
            double temperatura;
            long instante = 0;
            long lRecebido;
            String vRecebido;

// Formato antigo:
// REG;host;porta;p;seq;sensor;temp;L;V
//
// Formato novo:
// REG;host;porta;seq;sensor;temp;instante;L;V

            if (campos[4].trim().matches("\\d+")
                    && campos[3].trim().matches("\\d+")
                    && Integer.parseInt(campos[3].trim()) == particao) {

                // Formato antigo
                sequencia = Long.parseLong(campos[4].trim());
                sensor = campos[5].trim();
                temperatura = Double.parseDouble(campos[6].trim());
                lRecebido = Long.parseLong(campos[7].trim());
                vRecebido = campos[8].trim();

            } else {

                // Formato novo
                // A particao e identificada pelo topico PubSub.
                sequencia = Long.parseLong(campos[3].trim());
                sensor = campos[4].trim();
                temperatura = Double.parseDouble(campos[5].trim());
                instante = Long.parseLong(campos[6].trim());
                lRecebido = Long.parseLong(campos[7].trim());
                vRecebido = campos[8].trim();

                if (instante < 0) {
                    System.out.println("Instante invalido.");
                    return;
                }
            }
            String[] componentes = vRecebido.split(",");

            if (componentes.length != vetor.length || sequencia < 1) {
                System.out.println("Registo com vetor ou sequencia invalida.");
                return;
            }

            long[] recebido = new long[vetor.length];

            for (int i = 0; i < vetor.length; i++) {
                recebido[i] = Long.parseLong(componentes[i].trim());
                if (recebido[i] < 0) return;
            }

            long lAlarme;
            long[] vAlarme;

            synchronized (this) {
                if (!recebidos[particao].add(sequencia)) {
                    return;
                }

                // Acumula o conhecimento de TODOS os registos.
                lamport = Math.max(lamport, lRecebido);

                for (int i = 0; i < vetor.length; i++) {
                    vetor[i] = Math.max(vetor[i], recebido[i]);
                }

                System.out.println(
                        "Recebido: " + sensor
                                + " p" + particao + "#" + sequencia
                                + " temp=" + temperatura
                );

                // Evita alarmes sobre alarmes.
                if (sensor.startsWith("ALARM-")) {
                    return;
                }

                if (temperatura < LIMIAR_ALARME) {
                    return;
                }

                // Fotografia do conhecimento causal neste instante.
                // FALHA DELIBERADA 4: utiliza a referencia do vetor original
                // Fotografia do conhecimento causal neste instante.
                lAlarme = lamport;
                vAlarme = vetor.clone();
            }

            String sensorAlarme = "ALARM-" + sensor;
            int destino = partitioner.of(sensorAlarme);

            String mensagemAlarme =
                    "IN;" + sensorAlarme
                            + ";" + temperatura
                            + ";" + lAlarme
                            + ";" + vetorTexto(vAlarme);

            ipfs.publish(Wire.topicIn(destino), mensagemAlarme);

            System.out.println(
                    "ALARME: " + sensor
                            + " p" + particao + "#" + sequencia
                            + " -> p" + destino
                            + " L=" + lAlarme
                            + " V=" + vetorTexto(vAlarme)
            );

        } catch (Exception e) {
            System.err.println(
                    "Erro ao processar registo: " + e.getMessage()
            );
        }
    }

    private static String vetorTexto(long[] v) {
        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < v.length; i++) {
            if (i > 0) sb.append(',');
            sb.append(v[i]);
        }

        return sb.toString();
    }
}
