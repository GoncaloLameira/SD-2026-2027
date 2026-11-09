
package raft04;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CausalCheck {

    private static class Registo {
        int particao;
        long seq;
        long termo;
        String sensor;
        double temperatura;
        long instante;
        long lamport;
        long[] vetor;

        Registo(int particao, long seq, long termo,
                String sensor, double temperatura, long instante,
                long lamport, long[] vetor) {

            this.particao = particao;
            this.seq = seq;
            this.termo = termo;
            this.sensor = sensor;
            this.temperatura = temperatura;
            this.instante = instante;
            this.lamport = lamport;
            this.vetor = vetor;
        }

        String identificador() {
            return "p" + particao + "#" + seq + "(termo=" + termo + ")";
        }

        String chave() {
            return particao + ";" + seq + ";" + termo;
        }
    }

    private static final Pattern NOME_FICHEIRO =
            Pattern.compile("no-p(\\d+)-n(\\d+)\\.txt");

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

    private static void executar(int n, Path pasta)
            throws IOException {

        Map<String, Registo> unicos = new LinkedHashMap<>();

        long errosVetor = 0;
        long errosLeitura = 0;
        long copiasDuplicadas = 0;

        System.out.println("=== CAUSAL CHECK ===");
        System.out.println("Particoes: " + n);
        System.out.println("Pasta: " + pasta.toAbsolutePath());

        List<Path> ficheiros = new ArrayList<>();

        try (var paths = Files.list(pasta)) {
            paths.filter(Files::isRegularFile)
                    .filter(p -> NOME_FICHEIRO.matcher(
                            p.getFileName().toString()).matches())
                    .sorted()
                    .forEach(ficheiros::add);
        }

        if (ficheiros.isEmpty()) {
            throw new IOException(
                    "Nao foram encontrados ficheiros no-p<k>-n<i>.txt");
        }

        long[] quantidadePorParticao = new long[n];

        for (Path ficheiro : ficheiros) {

            Matcher matcher = NOME_FICHEIRO.matcher(
                    ficheiro.getFileName().toString());

            if (!matcher.matches()) {
                continue;
            }

            int k = Integer.parseInt(matcher.group(1));
            int no = Integer.parseInt(matcher.group(2));

            if (k < 1 || k > n) {
                System.out.println(
                        "AVISO: particao fora do intervalo: " + ficheiro);
                continue;
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

                    quantidade++;

                    // Verifica todos os ficheiros, incluindo as replicas.
                    if (r.vetor[k-1] != r.seq) {
                        errosVetor++;

                        System.out.println(
                                "VIOLACAO V[k]=seq: " + r.identificador()
                                        + " V[k]=" + r.vetor[k-1]
                                        + " ficheiro=" + ficheiro.getFileName());
                    }

                    // A mesma identidade (particao, seq, termo)
                    // representa o mesmo registo nas tres replicas.
                    if (unicos.putIfAbsent(r.chave(), r) != null) {
                        copiasDuplicadas++;
                    }

                } catch (Exception e) {
                    errosLeitura++;

                    System.out.println(
                            "ERRO no ficheiro " + ficheiro.getFileName()
                                    + " linha " + numeroLinha
                                    + ": " + e.getMessage());
                }
            }

            quantidadePorParticao[k-1] += quantidade;

            System.out.println(
                    "Particao " + k + ", no " + no
                            + ": " + quantidade + " registos");
        }

        List<Registo> todos = new ArrayList<>(unicos.values());

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
        System.out.println("Ficheiros analisados: " + ficheiros.size());
        System.out.println("Registos lidos (incluindo replicas): "
                + total(quantidadePorParticao));
        System.out.println("Registos logicos unicos: " + todos.size());
        System.out.println("Copias repetidas ignoradas na comparacao: "
                + copiasDuplicadas);

        System.out.println("Violacoes V[k]=seq: " + errosVetor);
        System.out.println("Erros de leitura: " + errosLeitura);

        System.out.println("Pares a -> b: " + causalAB);
        System.out.println("Pares b -> a: " + causalBA);
        System.out.println("Pares causais (total): "
                + (causalAB + causalBA));
        System.out.println("Pares concorrentes: " + concorrentes);
        System.out.println("Pares com vetores iguais: " + vetoresIguais);
        System.out.println("Violacoes de Lamport: " + violacoesLamport);

        System.out.println("Concorrentes com L distinto: "
                + concorrentesLDistinto);

        System.out.printf(Locale.US,
                "Proporcao de concorrentes com L distinto: %.2f%%%n",
                proporcao);

        // Vista ordenada por (L, k, seq).
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
                            + " | termo=" + r.termo
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

    private static long total(long[] valores) {
        long soma = 0;

        for (long valor : valores) {
            soma += valor;
        }

        return soma;
    }

    private static Registo analisarLinha(
            String linha, int k, int n) {

        String[] f = linha.trim().split(";", -1);

        if (f.length != 7) {
            throw new IllegalArgumentException(
                    "Esperados 7 campos: " + linha);
        }

        long seq = Long.parseLong(f[0].trim());
        long termo = Long.parseLong(f[1].trim());
        String sensor = f[2].trim();
        double temperatura = Double.parseDouble(f[3].trim());
        long instante = Long.parseLong(f[4].trim());
        long lamport = Long.parseLong(f[5].trim());

        if (seq < 1 || termo < 1 || sensor.isEmpty()
                || !Double.isFinite(temperatura)
                || instante < 0 || lamport < 0) {

            throw new IllegalArgumentException(
                    "Valores invalidos: " + linha);
        }

        String[] partesVetor = f[6].split(",", -1);

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
                k, seq, termo, sensor, temperatura,
                instante, lamport, vetor);
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
