package rep02;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class PartitionCheck {

    public static void main(String[] args) {

        if (args.length != 2) {
            System.out.println(
                    "Uso: java rep02.PartitionCheck <N> <pasta>"
            );
            return;
        }

        int n;

        try {
            n = Integer.parseInt(args[0]);
        } catch (NumberFormatException e) {
            System.out.println(
                    "Número de partições inválido: " + args[0]
            );
            return;
        }

        if (n < 1) {
            System.out.println(
                    "Número de partições inválido: " + n
            );
            return;
        }

        Path pasta = Paths.get(args[1]);
        Partitioner partitioner = new Partitioner(n);

        System.out.println("PartitionCheck");
        System.out.println("Partições: " + partitioner.count());
        System.out.println("Pasta: " + pasta);
        System.out.println();

        long totalRegistos = 0;

        // =========================================================
        // VERIFICAR CADA PARTIÇÃO
        // =========================================================

        for (int k = 0; k < n; k++) {

            Path ficheiroLeader =
                    pasta.resolve("lider-p" + k + ".txt");

            System.out.println("=== P" + k + " ===");

            if (!Files.exists(ficheiroLeader)) {
                System.out.println(
                        "Ficheiro em falta: " + ficheiroLeader
                );
                System.out.println();
                continue;
            }

            try {

                List<String> linhasLeader =
                        lerRegistos(ficheiroLeader);

                totalRegistos += linhasLeader.size();

                // =================================================
                // 1. SEQUÊNCIA + PARTIÇÃO DOS SENSORES
                // =================================================

                Set<String> sensores =
                        new HashSet<>();

                for (String linha : linhasLeader) {

                    SensorRecord r =
                            SensorRecord.fromLine(linha);

                    sensores.add(
                            r.getSensorId()
                    );

                    int particaoEsperada =
                            partitioner.of(
                                    r.getSensorId()
                            );

                    if (particaoEsperada != k) {

                        System.out.println(
                                "ERRO PARTIÇÃO: sensor " +
                                        r.getSensorId() +
                                        " está em P" + k +
                                        " mas pertence a P" +
                                        particaoEsperada
                        );
                    }
                }

                verificarSequencia(
                        linhasLeader,
                        "lider-p" + k + ".txt"
                );

                // =================================================
                // 2. DETERMINAR RÉPLICAS ESPERADAS
                // =================================================

                int[] replicas =
                        replicasEsperadas(n, k);

                if (replicas.length == 0) {

                    System.out.println(
                            "Sem atribuição de réplicas definida para P" + k
                    );

                    System.out.println();
                    continue;
                }

                // =================================================
                // 3. VERIFICAR CADA RÉPLICA
                // =================================================

                for (int replicaId : replicas) {

                    Path ficheiroReplica =
                            pasta.resolve(
                                    "replica-" +
                                            replicaId +
                                            "-p" +
                                            k +
                                            ".txt"
                            );

                    if (!Files.exists(ficheiroReplica)) {

                        System.out.println(
                                "ERRO: ficheiro em falta: " +
                                        ficheiroReplica
                        );

                        continue;
                    }

                    List<String> linhasReplica =
                            lerRegistos(
                                    ficheiroReplica
                            );

                    verificarSequencia(
                            linhasReplica,
                            "replica-" +
                                    replicaId +
                                    "-p" +
                                    k +
                                    ".txt"
                    );

                    verificarPrefixo(
                            linhasLeader,
                            linhasReplica,
                            "R" +
                                    replicaId +
                                    "/P" +
                                    k
                    );

                    System.out.println(
                            "Registos R" +
                                    replicaId +
                                    "/P" +
                                    k +
                                    ": " +
                                    linhasReplica.size()
                    );
                }

                // =================================================
                // 4. CONTAGEM DA PARTIÇÃO
                // =================================================

                System.out.println(
                        "Registos Leader P" +
                                k +
                                ": " +
                                linhasLeader.size()
                );

                System.out.println(
                        "Sensores distintos P" +
                                k +
                                ": " +
                                sensores.size()
                );

            } catch (Exception e) {

                System.out.println(
                        "Erro ao analisar P" +
                                k +
                                ": " +
                                e
                );
            }

            System.out.println();
        }

        // =========================================================
        // RESUMO GLOBAL
        // =========================================================

        System.out.println(
                "=== RESUMO GLOBAL ==="
        );

        System.out.println(
                "Total de registos do sistema: " +
                        totalRegistos
        );

        System.out.println(
                "Total = soma dos registos dos Leaders por partição."
        );
    }

    // =============================================================
    // RÉPLICAS ESPERADAS
    // =============================================================

    private static int[] replicasEsperadas(
            int n,
            int partitionId
    ) {

        /*
         * Experiência N=1:
         * P0 é mantida pelas três réplicas.
         */
        if (n == 1 && partitionId == 0) {
            return new int[]{1, 2, 3};
        }

        /*
         * Topologia normal N=3, R=2:
         *
         * P0 -> R1, R3
         * P1 -> R1, R2
         * P2 -> R2, R3
         */
        if (n == 3) {

            if (partitionId == 0) {
                return new int[]{1, 3};
            }

            if (partitionId == 1) {
                return new int[]{1, 2};
            }

            if (partitionId == 2) {
                return new int[]{2, 3};
            }
        }

        return new int[0];
    }

    // =============================================================
    // LER APENAS REGISTOS NÃO VAZIOS
    // =============================================================

    private static List<String> lerRegistos(
            Path ficheiro
    ) throws Exception {

        List<String> linhas =
                Files.readAllLines(ficheiro);

        List<String> registos =
                new ArrayList<>();

        for (String linha : linhas) {

            if (!linha.isBlank()) {
                registos.add(
                        linha.trim()
                );
            }
        }

        return registos;
    }

    // =============================================================
    // VERIFICAR SEQUÊNCIA
    // =============================================================

    private static boolean verificarSequencia(
            List<String> linhas,
            String nomeFicheiro
    ) {

        long esperado = 1;
        boolean ok = true;

        for (String linha : linhas) {

            SensorRecord r =
                    SensorRecord.fromLine(linha);

            if (r.getSeq() != esperado) {

                System.out.println(
                        "ERRO SEQUÊNCIA " +
                                nomeFicheiro +
                                ": esperado=" +
                                esperado +
                                " encontrado=" +
                                r.getSeq()
                );

                ok = false;
            }

            esperado = r.getSeq() + 1;
        }

        return ok;
    }

    // =============================================================
    // VERIFICAR SE A RÉPLICA É IGUAL OU PREFIXO DO LEADER
    // =============================================================

    private static boolean verificarPrefixo(
            List<String> leader,
            List<String> replica,
            String nomeReplica
    ) {

        if (replica.size() > leader.size()) {

            System.out.println(
                    "ERRO CONTEÚDO: " +
                            nomeReplica +
                            " tem mais registos que o Leader"
            );

            return false;
        }

        for (int i = 0; i < replica.size(); i++) {

            if (!replica.get(i).equals(leader.get(i))) {

                System.out.println(
                        "ERRO CONTEÚDO: " +
                                nomeReplica +
                                " difere do Leader no registo " +
                                (i + 1)
                );

                return false;
            }
        }

        int faltam =
                leader.size() - replica.size();

        if (faltam == 0) {

            System.out.println(
                    "OK: " +
                            nomeReplica +
                            " igual ao Leader"
            );

        } else {

            System.out.println(
                    "OK: " +
                            nomeReplica +
                            " é prefixo do Leader; faltam " +
                            faltam +
                            " registos"
            );
        }

        return true;
    }
}