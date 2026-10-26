
package rep03;

import java.util.Arrays;

public class SensorRecord {

    private final int partition;
    private final long sequence;
    private final String sensorId;
    private final double temperature;
    private final long instante;
    private final int lamport;
    private final int[] vectorClock;

    // Construtor de compatibilidade com o codigo existente
    public SensorRecord(int partition, long sequence,
                        String sensorId, double temperature,
                        int lamport, int[] vectorClock) {

        this(partition, sequence, sensorId, temperature,
                System.currentTimeMillis(), lamport, vectorClock);
    }

    // Construtor com instante
    public SensorRecord(int partition, long sequence,
                        String sensorId, double temperature,
                        long instante, int lamport, int[] vectorClock) {

        this.partition = partition;
        this.sequence = sequence;
        this.sensorId = sensorId;
        this.temperature = temperature;
        this.instante = instante;
        this.lamport = lamport;
        this.vectorClock = Arrays.copyOf(
                vectorClock, vectorClock.length);
    }

    public int getPartition() {
        return partition;
    }

    public long getSequence() {
        return sequence;
    }

    public String getSensorId() {
        return sensorId;
    }

    public double getTemperature() {
        return temperature;
    }
    public long getInstante() {
        return instante;
    }
    public int getLamport() {
        return lamport;
    }

    public int[] getVectorClock() {
        return Arrays.copyOf(vectorClock, vectorClock.length);
    }

    // Formato para armazenamento e recuperacao TCP

    public String toLine() {
        StringBuilder vector = new StringBuilder();

        for (int i = 0; i < vectorClock.length; i++) {
            if (i > 0) {
                vector.append(",");
            }
            vector.append(vectorClock[i]);
        }

        return sequence + ";"
                + sensorId + ";"
                + temperature + ";"
                + instante + ";"
                + lamport + ";"
                + vector;
    }


    // Reconstrói um registo a partir de uma linha
    public static SensorRecord fromLine(String line) {


        String[] fields = line.trim().split(";", -1);

        if (fields.length != 6) {
            throw new IllegalArgumentException(
                    "Registo mal formado: " + line
            );
        }

        String[] vectorFields = fields[5].split(",");
        int[] vector = new int[vectorFields.length];

        for (int i = 0; i < vectorFields.length; i++) {
            vector[i] = Integer.parseInt(vectorFields[i].trim());
        }

        return new SensorRecord(
                Integer.parseInt(fields[0]),
                Long.parseLong(fields[1]),
                fields[2],
                Double.parseDouble(fields[3]),
                Integer.parseInt(fields[4]),
                vector
        );
    }
    // Leitura do novo formato: seq;sensor;temperatura;instante;L;V
// A particao e identificada pelo nome do ficheiro.
    public static SensorRecord fromLine(String line, int partition) {

        String[] fields = line.trim().split(";", -1);

        if (fields.length != 6) {
            throw new IllegalArgumentException(
                    "Registo mal formado: " + line);
        }

        String[] vectorFields = fields[5].split(",");
        int[] vector = new int[vectorFields.length];

        for (int i = 0; i < vectorFields.length; i++) {
            vector[i] = Integer.parseInt(vectorFields[i].trim());
        }

        return new SensorRecord(
                partition,
                Long.parseLong(fields[0].trim()),
                fields[1].trim(),
                Double.parseDouble(fields[2].trim()),
                Long.parseLong(fields[3].trim()),
                Integer.parseInt(fields[4].trim()),
                vector
        );
    }
    @Override
    public String toString() {
        return "REG | p=" + partition
                + " | seq=" + sequence
                + " | sensor=" + sensorId
                + " | temp=" + temperature
                + " | L=" + lamport
                + " | V=" + Arrays.toString(vectorClock);
    }
}
