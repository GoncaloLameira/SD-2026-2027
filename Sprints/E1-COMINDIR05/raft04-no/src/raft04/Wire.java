
package raft04;

import java.nio.file.Files;
import java.nio.file.Path;

public final class Wire {

    public static final int PARTITIONS = 3;
    public static final int GRUPO = 3;
    public static final int LIMITE_RETIDOS = 10;
    public static final int T_TCP_MS = 5000;
    public static final int PORTO_BASE = 7000;

    // Periodo do heartbeat/lote (ms)
    public static final int PERIODO_HB_MS =
            Integer.getInteger("raft04.periodo", 2000);

    // Timeout do detetor de falhas
    public static final int FATOR_TIMEOUT =
            Integer.getInteger("raft04.fator", 3);

    // Intervalo aleatorio para iniciar uma eleicao
    public static final int ELEICAO_MIN_MS =
            Integer.getInteger("raft04.eleicaoMin", 3 * PERIODO_HB_MS);

    public static final int ELEICAO_MAX_MS =
            Integer.getInteger("raft04.eleicaoMax", 6 * PERIODO_HB_MS);

    // Periodo da thread de verificacao (ms)
    public static final int VERIFICACAO_MS = 200;

    private Wire() {
    }

    // Topico de entrada das leituras do produtor
    public static String topicIn(int partition) {
        return "raft04-in-p" + partition;
    }

    // Topico de publicacao dos lotes
    public static String topicData(int partition) {
        return "raft04-p" + partition;
    }

    // Topico de mensagens de eleicao
    public static String topicElection(int partition) {
        return "raft04-eleicao-p" + partition;
    }

    // Porto TCP de recuperacao de cada no
    public static int nodePort(int partition, int node) {
        return PORTO_BASE + (partition - 1) * GRUPO + (node - 1);
    }

    // Verifica se o no esta isolado
    public static boolean isolated(int partition, int node) {
        return Files.exists(
                Path.of("isolar-p" + partition + "-n" + node)
        );
    }

    // Mensagem enviada pelo produtor
    public static String in(String sensor, double temperature) {
        return "IN;" + sensor + ";" + temperature + ";0;0,0,0";
    }

    // Identifica o tipo da mensagem
    public static String type(String message) {
        String m = message.trim();
        int i = m.indexOf(';');

        if (i <= 0) {
            throw new IllegalArgumentException(
                    "Mensagem sem tipo: " + message
            );
        }

        return m.substring(0, i);
    }

    // Extrai o conteudo da mensagem
    public static String body(String message) {
        String m = message.trim();
        int i = m.indexOf(';');

        if (i < 0) {
            throw new IllegalArgumentException(
                    "Mensagem mal formada: " + message
            );
        }

        return m.substring(i + 1);
    }
}
