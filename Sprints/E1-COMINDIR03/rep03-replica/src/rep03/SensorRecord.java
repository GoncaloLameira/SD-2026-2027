
package rep03;

import java.util.Arrays;

public class SensorRecord {

    private final int partition;
    private final long sequence;
    private final String sensorId;
    private final double temperature;
    private final int lamport;
    private final int[] vectorClock;

    public SensorRecord(int partition, long sequence,
                        String sensorId, double temperature,
                        int lamport, int[] vectorClock) {

        this.partition = partition;
        this.sequence = sequence;
        this.sensorId = sensorId;
        this.temperature = temperature;
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

    public int getLamport() {
        return lamport;
    }

    public int[] getVectorClock() {
        return Arrays.copyOf(vectorClock, vectorClock.length);
    }

    // Formato utilizado para guardar e recuperar registos por TCP
    public String toLine() {
        StringBuilder vetor = new StringBuilder();

        for (int i = 0; i < vectorClock.length; i++) {
            if (i > 0) {
                vetor.append(",");
            }
            vetor.append(vectorClock[i]);
        }

        return partition + ";"
                + sequence + ";"
                + sensorId + ";"
                + temperature + ";"
                + lamport + ";"
                + vetor;
    }

    // Reconstrói um registo a partir do formato separado por ;
    public static SensorRecord fromLine(String line) {
        String[] campos = line.split(";", -1);

        if (campos.length != 6) {
            throw new IllegalArgumentException(
                    "Formato de registo invalido: " + line);
        }

        String[] valoresVetor = campos[5].split(",");
        int[] vetor = new int[valoresVetor.length];

        for (int i = 0; i < valoresVetor.length; i++) {
            vetor[i] = Integer.parseInt(valoresVetor[i].trim());
        }

        return new SensorRecord(
                Integer.parseInt(campos[0].trim()),
                Long.parseLong(campos[1].trim()),
                campos[2],
                Double.parseDouble(campos[3].trim()),
                Integer.parseInt(campos[4].trim()),
                vetor
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
