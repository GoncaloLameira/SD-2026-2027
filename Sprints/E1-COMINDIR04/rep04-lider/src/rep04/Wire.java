package rep04;

public final class Wire {

    public static final int PARTITIONS = 3;
    public static final int LIMITE_RETIDOS = 10;
    public static final int T_TCP_MS = 5000;
    public static final int PORTO_BASE = 7000;

    // Periodo do heartbeat/lote (ms); alteravel com -Drep04.periodo=...
    public static final int PERIODO_HB_MS =
            Integer.getInteger("rep04.periodo", 5000);

    // TIMEOUT = FATOR_TIMEOUT x PERIODO_HB_MS; alteravel com -Drep04.fator=...
    public static final int FATOR_TIMEOUT =
            Integer.getInteger("rep04.fator", 3);

    // Periodo da thread de verificacao do detetor (ms)
    public static final int VERIFICACAO_MS = 1000;

    private Wire() {
    }

    // Topico onde o lider recebe leituras do produtor
    public static String topicIn(int partition) {
        return "rep04-in-p" + partition;
    }

    // Topico onde o lider publica os lotes (heartbeat + registos)
    public static String topicData(int partition) {
        return "rep04-p" + partition;
    }

    // Mensagem enviada pelo produtor com relogios iniciais
    public static String in(String sensor, double temperature) {
        return "IN;" + sensor + ";" + temperature
                + ";0;0,0,0";
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
