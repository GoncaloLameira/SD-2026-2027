
package rep03;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;

public class CausalCheck {

    private static class Registo {
        int particao;
        long seq;
        String sensor;
        double temperatura;
        String instante;
        long lamport;
        long[] vetor;
        String linha;

        Registo(int particao, long seq, String sensor,
                double temperatura, String instante,
                long lamport, long[] vetor, String linha) {
            this.particao = particao;
            this.seq = seq;
            this.sensor = sensor;
            this.temperatura = temperatura;
            this.instante = instante;
            this.lamport = lamport;
            this.vetor = vetor;
            this.linha = linha;
        }

        String identificador() {
            return "p" + particao + "#" + seq;
        }
    }

    public static void main(String[] args) {

        if (args.length != 2) {
            System.out.println("Uso: CausalCheck <N> <pasta>");
            return;
        }

        try {
            int n = Integer.parseInt(args[0]);

            if (n <= 0) {
                throw new IllegalArgumentException(
                        "Numero de particoes invalido.");
            }

            Path pasta = Path.of(args[1]);

            if (!Files.isDirectory(pasta)) {
                throw new IOException(
                        "Pasta nao encontrada: " + pasta.toAbsolutePath());
            }

            executar(n, pasta);

        } catch (Exception e) {
            System.err.println("Erro: " + e.getMessage());
        }
    }

    private static void executar(int n, Path pasta) throws IOException {

        List<Registo> todos = new ArrayList<>();

        long errosVetor = 0;
        long errosLeitura = 0;

        System.out.println("=== CAUSAL CHECK ===");
        System.out.println("Particoes: " + n);
        System.out.println("Pasta: " + pasta.toAbsolutePath());

        for (int k = 0; k < n; k++) {

            Path ficheiro = pasta.resolve("lider-p" + k + ".txt");

            if (!Files.exists(ficheiro)) {
                throw new IOException(
                        "Ficheiro nao encontrado: " + ficheiro);
            }

            int quantidade = 0;
            int numeroLinha = 0;

            for (String linha : Files.readAllLines(
                    ficheiro, StandardCharsets.UTF_8)) {

                numeroLinha++;

                if (linha.isBlank()) {
                    continue;
                }

                try {
                    Registo r = analisarLinha(linha, k, n);
                    todos.add(r);
                    quantidade++;

                    if (r.vetor[k] != r.seq) {
                        errosVetor++;
                        System.out.println(
                                "VIOLACAO V[k]=seq: "
                                        + r.identificador()
                                        + " V[k]=" + r.vetor[k]);
                    }

                } catch (Exception e) {
                    errosLeitura++;
                    System.out.println(
                            "ERRO no ficheiro " + ficheiro.getFileName()
                                    + " linha " + numeroLinha
                                    + ": " + e.getMessage());
                }
            }

            System.out.println(
                    "Particao " + k + ": " + quantidade + " registos");
        }

        long causalAB = 0;
        long causalBA = 0;
        long concorrentes = 0;
        long concorrentesLDistinto = 0;
        long violacoesLamport = 0;
        long vetoresIguais = 0;

        // Compara apenas pares de particoes diferentes.
        for (int i = 0; i < todos.size(); i++) {

            Registo a = todos.get(i);

            for (int j = i + 1; j < todos.size(); j++) {

                Registo b = todos.get(j);

                if (a.particao == b.particao) {
                    continue;
                }

                int relacao = comparar(a.vetor, b.vetor);

                if (relacao == -1) {
                    causalAB++;

                    if (a.lamport >= b.lamport) {
                        violacoesLamport++;
                        System.out.println(
                                "VIOLACAO L: " + a.identificador()
                                        + " -> " + b.identificador()
                                        + " mas L=" + a.lamport
                                        + " e L=" + b.lamport);
                    }

                } else if (relacao == 1) {
                    causalBA++;

                    if (b.lamport >= a.lamport) {
                        violacoesLamport++;
                        System.out.println(
                                "VIOLACAO L: " + b.identificador()
                                        + " -> " + a.identificador()
                                        + " mas L=" + b.lamport
                                        + " e L=" + a.lamport);
                    }

                } else if (relacao == 2) {
                    concorrentes++;

                    if (a.lamport != b.lamport) {
                        concorrentesLDistinto++;
                    }

                } else {
                    vetoresIguais++;
                    System.out.println(
                            "AVISO: vetores iguais em "
                                    + a.identificador()
                                    + " e " + b.identificador());
                }
            }
        }

        double proporcao = concorrentes == 0
                ? 0.0
                : 100.0 * concorrentesLDistinto / concorrentes;

        System.out.println();
        System.out.println("=== RESULTADOS ===");
        System.out.println("Total de registos: " + todos.size());
        System.out.println("Violacoes V[k]=seq: " + errosVetor);
        System.out.println("Erros de leitura: " + errosLeitura);

        System.out.println("Pares a -> b: " + causalAB);
        System.out.println("Pares b -> a: " + causalBA);
        System.out.println("Pares causais (total): "
                + (causalAB + causalBA));
        System.out.println("Pares concorrentes: " + concorrentes);
        System.out.println("Pares com vetores iguais: " + vetoresIguais);
        System.out.println("Violacoes de Lamport: " + violacoesLamport);

        System.out.println(
                "Concorrentes com L distinto: "
                        + concorrentesLDistinto);

        System.out.printf(Locale.US,
                "Proporcao de concorrentes com L distinto: %.2f%%%n",
                proporcao);

        // Vista ordenada por (L, k).
        todos.sort(
                Comparator.comparingLong((Registo r) -> r.lamport)
                        .thenComparingInt(r -> r.particao)
                        .thenComparingLong(r -> r.seq)
        );

        List<String> vista = new ArrayList<>();

        for (Registo r : todos) {
            vista.add(
                    "p=" + r.particao
                            + " | seq=" + r.seq
                            + " | sensor=" + r.sensor
                            + " | temp=" + r.temperatura
                            + " | L=" + r.lamport
                            + " | V=" + vetorTexto(r.vetor)
            );
        }

        Path destino = pasta.resolve("vista-lamport.txt");

        Files.write(destino, vista, StandardCharsets.UTF_8);

        System.out.println();
        System.out.println(
                "Vista de Lamport criada: " + destino.toAbsolutePath());
    }

    private static Registo analisarLinha(
            String linha, int k, int n) {

        String[] f = linha.trim().split(";", -1);

        if (f.length != 6) {
            throw new IllegalArgumentException(
                    "Esperados 6 campos: " + linha);
        }

        long seq;
        String sensor;
        double temperatura;
        String instante = "";
        long lamport;
        String vetorTexto;

        // Formato atual:
        // particao;seq;sensor;temperatura;L;V
        if (f[1].trim().matches("\\d+")) {

            int particaoLida = Integer.parseInt(f[0].trim());

            if (particaoLida != k) {
                throw new IllegalArgumentException(
                        "Particao diferente do ficheiro: " + linha);
            }

            seq = Long.parseLong(f[1].trim());
            sensor = f[2].trim();
            temperatura = Double.parseDouble(f[3].trim());
            lamport = Long.parseLong(f[4].trim());
            vetorTexto = f[5];

        } else {
            // Formato do enunciado:
            // seq;sensor;temperatura;instante;L;V

            seq = Long.parseLong(f[0].trim());
            sensor = f[1].trim();
            temperatura = Double.parseDouble(f[2].trim());
            instante = f[3].trim();
            lamport = Long.parseLong(f[4].trim());
            vetorTexto = f[5];
        }

        if (seq < 1 || lamport < 0
                || sensor.isEmpty()
                || !Double.isFinite(temperatura)) {
            throw new IllegalArgumentException(
                    "Valores invalidos: " + linha);
        }

        String[] partesVetor = vetorTexto.split(",", -1);

        if (partesVetor.length != n) {
            throw new IllegalArgumentException(
                    "Dimensao do vetor invalida: " + linha);
        }

        long[] vetor = new long[n];

        for (int i = 0; i < n; i++) {
            vetor[i] = Long.parseLong(partesVetor[i].trim());

            if (vetor[i] < 0) {
                throw new IllegalArgumentException(
                        "Vetor negativo: " + linha);
            }
        }

        return new Registo(
                k, seq, sensor, temperatura,
                instante, lamport, vetor, linha);
    }

    // -1: a antes de b
    //  1: b antes de a
    //  2: concorrentes
    //  0: iguais
    private static int comparar(long[] a, long[] b) {

        boolean menor = false;
        boolean maior = false;

        for (int i = 0; i < a.length; i++) {
            if (a[i] < b[i]) menor = true;
            if (a[i] > b[i]) maior = true;
        }

        if (menor && maior) return 2;
        if (menor) return -1;
        if (maior) return 1;

        return 0;
    }

    private static String vetorTexto(long[] vetor) {

        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < vetor.length; i++) {
            if (i > 0) sb.append(",");
            sb.append(vetor[i]);
        }

        return sb.toString();
    }
}
