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

    public SensorRecord(int partition, long sequence, long term,
                        String sensorId, double temperature,
                        long instante, int lamport, int[] vectorClock) {

        if (partition < 1 || partition > Wire.PARTITIONS) {
            throw new IllegalArgumentException("Partição inválida.");
        }

        if (sequence < 1 || term < 0) {
            throw new IllegalArgumentException(
                    "Sequência ou termo inválido."
            );
        }

        if (sensorId == null || sensorId.isBlank()) {
            throw new IllegalArgumentException(
                    "Identificador do sensor inválido."
            );
        }

        if (!Double.isFinite(temperature)) {
            throw new IllegalArgumentException(
                    "Temperatura inválida."
            );
        }

        if (instante < 0 || lamport < 0) {
            throw new IllegalArgumentException(
                    "Instante ou relógio de Lamport inválido."
            );
        }

        if (vectorClock == null
                || vectorClock.length != Wire.PARTITIONS) {
            throw new IllegalArgumentException(
                    "Vector clock inválido."
            );
        }

        for (int valor : vectorClock) {
            if (valor < 0) {
                throw new IllegalArgumentException(
                        "Vector clock contém valores negativos."
                );
            }
        }

        this.partition = partition;
        this.sequence = sequence;
        this.term = term;
        this.sensorId = sensorId;
        this.temperature = temperature;
        this.instante = instante;
        this.lamport = lamport;
        this.vectorClock = Arrays.copyOf(
                vectorClock,
                vectorClock.length
        );
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
        return Arrays.copyOf(
                vectorClock,
                vectorClock.length
        );
    }

    // Formato: seq;termo;sensor;temperatura;instante;L;V
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

    public static SensorRecord fromLine(
            String line,
            int partition
    ) {
        if (line == null || line.isBlank()) {
            throw new IllegalArgumentException(
                    "Linha de registo vazia."
            );
        }

        String[] f = line.trim().split(";", -1);

        if (f.length != 7) {
            throw new IllegalArgumentException(
                    "Registo mal formado: " + line
            );
        }

        String[] vf = f[6].split(",", -1);

        if (vf.length != Wire.PARTITIONS) {
            throw new IllegalArgumentException(
                    "Vector clock mal formado: " + line
            );
        }

        int[] vector = new int[vf.length];

        for (int i = 0; i < vf.length; i++) {
            vector[i] = Integer.parseInt(vf[i].trim());
        }

        return new SensorRecord(
                partition,
                Long.parseLong(f[0].trim()),
                Long.parseLong(f[1].trim()),
                f[2].trim(),
                Double.parseDouble(f[3].trim()),
                Long.parseLong(f[4].trim()),
                Integer.parseInt(f[5].trim()),
                vector
        );
    }

    @Override
    public String toString() {
        return "REG | p=" + partition
                + " | seq=" + sequence
                + " | termo=" + term
                + " | sensor=" + sensorId
                + " | temp=" + temperature
                + " | L=" + lamport
                + " | V=" + Arrays.toString(vectorClock);
    }
}