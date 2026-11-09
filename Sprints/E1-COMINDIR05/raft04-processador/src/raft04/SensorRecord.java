
package raft04;

import java.util.Arrays;

public class SensorRecord {

    private final int partition;
    private final long sequence;
    private final long term;
    private final String sensorId;
    private final double temperature;
    private final long instante;
    private final int lamport;
    private final int[] vectorClock;

    // Construtor de compatibilidade.
    public SensorRecord(
            int partition,
            long sequence,
            String sensorId,
            double temperature,
            int lamport,
            int[] vectorClock) {

        this(partition, sequence, 0, sensorId, temperature,
                System.currentTimeMillis(), lamport, vectorClock);
    }

    // Construtor com instante.
    public SensorRecord(
            int partition,
            long sequence,
            String sensorId,
            double temperature,
            long instante,
            int lamport,
            int[] vectorClock) {

        this(partition, sequence, 0, sensorId, temperature,
                instante, lamport, vectorClock);
    }

    // Construtor completo com termo Raft.
    public SensorRecord(
            int partition,
            long sequence,
            long term,
            String sensorId,
            double temperature,
            long instante,
            int lamport,
            int[] vectorClock) {

        this.partition = partition;
        this.sequence = sequence;
        this.term = term;
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

    public long getTerm() {
        return term;
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

    // Formato:
    // seq;term;sensor;temperatura;instante;L;V
    public String toLine() {

        StringBuilder vector = new StringBuilder();

        for (int i = 0; i < vectorClock.length; i++) {
            if (i > 0) {
                vector.append(",");
            }
            vector.append(vectorClock[i]);
        }

        return sequence + ";"
                + term + ";"
                + sensorId + ";"
                + temperature + ";"
                + instante + ";"
                + lamport + ";"
                + vector;
    }

    // Reconstrói um registo a partir de uma linha.
    public static SensorRecord fromLine(String line) {
        return fromLine(line, 0);
    }

    // Reconstrói um registo identificando a partição.
    public static SensorRecord fromLine(
            String line, int partition) {

        String[] fields = line.trim().split(";", -1);

        if (fields.length != 7) {
            throw new IllegalArgumentException(
                    "Registo mal formado: " + line);
        }

        long sequence = Long.parseLong(fields[0].trim());
        long term = Long.parseLong(fields[1].trim());
        String sensor = fields[2].trim();
        double temperature = Double.parseDouble(fields[3].trim());
        long instante = Long.parseLong(fields[4].trim());
        int lamport = Integer.parseInt(fields[5].trim());

        String[] vectorFields = fields[6].split(",", -1);
        int[] vector = new int[vectorFields.length];

        for (int i = 0; i < vectorFields.length; i++) {
            vector[i] = Integer.parseInt(
                    vectorFields[i].trim());
        }

        if (sequence < 1 || term < 1
                || sensor.isBlank()
                || !Double.isFinite(temperature)
                || instante < 0 || lamport < 0) {
            throw new IllegalArgumentException(
                    "Valores invalidos no registo: " + line);
        }

        return new SensorRecord(
                partition,
                sequence,
                term,
                sensor,
                temperature,
                instante,
                lamport,
                vector
        );
    }

    @Override
    public String toString() {
        return "REG | p=" + partition
                + " | seq=" + sequence
                + " | term=" + term
                + " | sensor=" + sensorId
                + " | temp=" + temperature
                + " | L=" + lamport
                + " | V=" + Arrays.toString(vectorClock);
    }
}
