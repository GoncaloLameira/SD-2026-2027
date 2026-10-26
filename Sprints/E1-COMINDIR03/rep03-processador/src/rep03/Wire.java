
package rep03;

public final class Wire {

    public static final int PARTITIONS = 3;
    public static final int LIMITE_RETIDOS = 10;
    public static final int T_TCP_MS = 5000;
    public static final int PORTO_BASE = 7000;

    private Wire() {
    }

    // Topico onde o lider recebe leituras do produtor
    public static String topicIn(int partition) {
        return "rep03-in-p" + partition;
    }

    // Topico onde o lider publica os registos
    public static String topicData(int partition) {
        return "rep03-p" + partition;
    }

    // Mensagem enviada pelo produtor

    // Mensagem enviada pelo produtor com relogios iniciais
    public static String in(String sensor, double temperature) {
        return "IN;" + sensor + ";" + temperature
                + ";0;0,0,0";
    }


    // Mensagem publicada pelo lider
    public static String reg(String host, int port, SensorRecord record) {
        return "REG;" + host + ";" + port + ";" + record.toLine();
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

    // Separa endereco, porta e registo
    public static String[] regParts(String message) {
        String[] fields = message.trim().split(";", 4);

        if (fields.length != 4 || !fields[0].equals("REG")) {
            throw new IllegalArgumentException(
                    "REG mal formado: " + message
            );
        }

        return new String[]{
                fields[1],
                fields[2],
                fields[3]
        };
    }
}
