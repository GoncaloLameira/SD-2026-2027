
package raft04;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class RaftCheck {

    private static final int PARTITIONS = 3;
    private static final int GROUP_SIZE = 3;

    private static final long MARGEM_EXECUCAO_MS = 60_000L;

    private static final Pattern NODE_FILE =
            Pattern.compile("no-p(\\d+)-n(\\d+)\\.txt");

    private static final Pattern EVENT_FILE =
            Pattern.compile("eventos-p(\\d+)-n(\\d+)\\.txt");

    private static final Pattern HB_FILE =
            Pattern.compile("hb-p(\\d+)-n(\\d+)\\.txt");

    private static final Pattern TERM_PATTERN =
            Pattern.compile("(?:termo|term)=([0-9]+)");

    private static class Registo {
        int partition;
        long sequence;
        long term;
        String sensor;
        double temperature;
        long instante;
        long lamport;
        String vector;

        Registo(int partition, String line) {
            String[] f = line.trim().split(";", -1);

            if (f.length != 7) {
                throw new IllegalArgumentException(
                        "Esperados 7 campos no registo: " + line);
            }

            this.partition = partition;
            this.sequence = Long.parseLong(f[0].trim());
            this.term = Long.parseLong(f[1].trim());
            this.sensor = f[2].trim();
            this.temperature = Double.parseDouble(f[3].trim());
            this.instante = Long.parseLong(f[4].trim());
            this.lamport = Long.parseLong(f[5].trim());
            this.vector = f[6].trim();

            if (sequence < 1 || term < 1 || sensor.isEmpty()
                    || !Double.isFinite(temperature)
                    || instante < 0 || lamport < 0) {
                throw new IllegalArgumentException(
                        "Valores invalidos: " + line);
            }
        }

        String identidade() {
            return partition + ";" + sequence + ";" + term;
        }

        String conteudo() {
            return sensor + ";"
                    + String.format(Locale.US, "%.2f", temperature)
                    + ";" + instante + ";" + lamport + ";" + vector;
        }

        String leitura() {
            return partition + ";" + sensor + ";"
                    + String.format(Locale.US, "%.2f", temperature);
        }
    }

    private static class LeituraProdutor {
        long instante;
        int partition;
        String sensor;
        double temperature;

        LeituraProdutor(String line) {
            String[] f = line.trim().split(";", -1);

            if (f.length != 4) {
                throw new IllegalArgumentException(
                        "Esperados 4 campos em produtor.txt: " + line);
            }

            instante = Long.parseLong(f[0].trim());
            partition = Integer.parseInt(f[1].trim());
            sensor = f[2].trim();
            temperature = Double.parseDouble(f[3].trim());

            if (instante < 0
                    || partition < 1 || partition > PARTITIONS
                    || sensor.isEmpty()
                    || !Double.isFinite(temperature)) {
                throw new IllegalArgumentException(
                        "Leitura invalida: " + line);
            }
        }

        String chave() {
            return partition + ";" + sensor + ";"
                    + String.format(Locale.US, "%.2f", temperature);
        }
    }

    private static class Evento {
        long timestamp;
        int partition;
        int node;
        long term;
        String tipo;
        String detalhe;

        Evento(long timestamp, int partition, int node,
               String tipo, String detalhe) {
            this.timestamp = timestamp;
            this.partition = partition;
            this.node = node;
            this.tipo = tipo;
            this.detalhe = detalhe;
            this.term = extrairTermo(detalhe);
        }
    }

    private static class Estatisticas {
        long candidatos;
        long votos;
        long lideres;
        long demissoes;
        long termos;
        long novosLideres;
        long lotes;
        long recebidos;
        long truncados;
        long descartados;
        long erros;
        long suspeitas;
        long recuperacoes;
        long outros;
    }

    private static class Eleicao {
        Set<Integer> candidatos = new TreeSet<>();
        Set<Integer> lideres = new TreeSet<>();
        Map<Integer, Set<Integer>> votosConcedidos = new TreeMap<>();
        Map<Integer, Set<Integer>> votosRecusados = new TreeMap<>();
        long primeiroCandidato = Long.MAX_VALUE;
        long primeiroLider = Long.MAX_VALUE;
    }

    private static class Heartbeat {
        long term;
        long heartbeat;
        long timestamp;
        long lastSeq;
        long lastTerm;
        long count;
        long bytes;

        Heartbeat(String line) {
            String[] f = line.trim().split(";", -1);

            if (f.length < 7) {
                throw new IllegalArgumentException(
                        "Esperados 7 campos em heartbeat: " + line);
            }

            term = Long.parseLong(f[0].trim());
            heartbeat = Long.parseLong(f[1].trim());
            timestamp = Long.parseLong(f[2].trim());
            lastSeq = Long.parseLong(f[3].trim());
            lastTerm = Long.parseLong(f[4].trim());
            count = Long.parseLong(f[5].trim());
            bytes = Long.parseLong(f[6].trim());
        }
    }

    public static void main(String[] args) {

        Path pasta;
        Path falhas = null;

        if (args.length == 1) {
            pasta = Path.of(args[0]);
        } else if (args.length == 2) {
            pasta = Path.of(args[1]);
        } else if (args.length >= 3) {
            pasta = Path.of(args[1]);
            falhas = Path.of(args[2]);
        } else {
            System.out.println("Uso: RaftCheck <pasta>");
            System.out.println("Ou: RaftCheck <N> <pasta> [falhas.txt]");
            return;
        }

        try {
            if (!Files.isDirectory(pasta)) {
                throw new IOException(
                        "Pasta nao encontrada: " + pasta.toAbsolutePath());
            }

            executar(pasta, falhas);

        } catch (Exception e) {
            System.err.println("Erro no RaftCheck: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static void executar(Path pasta, Path falhasArgumento)
            throws IOException {

        System.out.println("==================================");
        System.out.println("             RAFT CHECK");
        System.out.println("==================================");
        System.out.println("Pasta: " + pasta.toAbsolutePath());
        System.out.println();

        Map<Integer, Map<Integer, TreeMap<Long, Registo>>> dados =
                new TreeMap<>();

        Map<String, Registo> registosUnicos = new HashMap<>();
        Map<String, String> conteudos = new HashMap<>();

        Map<Integer, Map<Integer, TreeSet<Long>>> sequencias =
                new TreeMap<>();

        Map<Integer, Estatisticas> estatisticas = new TreeMap<>();
        Map<Integer, Map<Long, Eleicao>> eleicoes = new TreeMap<>();

        List<Evento> todosEventos = new ArrayList<>();

        long registosLidos = 0;
        long errosLeitura = 0;
        long duplicados = 0;
        long divergencias = 0;
        long lacunas = 0;

        List<Path> ficheirosNos = listar(pasta, NODE_FILE);
        List<Path> ficheirosEventos = listar(pasta, EVENT_FILE);
        List<Path> ficheirosHeartbeat = listar(pasta, HB_FILE);

        // ---------------------------------------------------------
        // 1. FICHEIROS DOS NOS
        // ---------------------------------------------------------

        System.out.println("=== 1. FICHEIROS DOS NOS ===");

        for (Path ficheiro : ficheirosNos) {

            Matcher matcher = NODE_FILE.matcher(
                    ficheiro.getFileName().toString());

            if (!matcher.matches()) {
                continue;
            }

            int p = Integer.parseInt(matcher.group(1));
            int no = Integer.parseInt(matcher.group(2));

            if (!particaoValida(p) || !noValido(no)) {
                System.out.println(
                        "AVISO: Ficheiro fora do intervalo: "
                                + ficheiro.getFileName());
                continue;
            }

            dados.putIfAbsent(p, new TreeMap<>());
            dados.get(p).putIfAbsent(no, new TreeMap<>());

            sequencias.putIfAbsent(p, new TreeMap<>());
            sequencias.get(p).putIfAbsent(no, new TreeSet<>());

            long quantidade = 0;
            int linhaNumero = 0;

            for (String linha : Files.readAllLines(
                    ficheiro, StandardCharsets.UTF_8)) {

                linhaNumero++;

                if (linha.isBlank()) {
                    continue;
                }

                try {
                    Registo r = new Registo(p, linha);

                    quantidade++;
                    registosLidos++;

                    dados.get(p).get(no).put(r.sequence, r);
                    sequencias.get(p).get(no).add(r.sequence);

                    String identidade = r.identidade();
                    String conteudo = r.conteudo();

                    if (conteudos.containsKey(identidade)) {
                        duplicados++;

                        if (!conteudos.get(identidade).equals(conteudo)) {
                            divergencias++;

                            System.out.println(
                                    "DIVERGENCIA: " + identidade
                                            + " tem conteudos diferentes.");
                        }
                    } else {
                        conteudos.put(identidade, conteudo);
                        registosUnicos.put(identidade, r);
                    }

                } catch (Exception e) {
                    errosLeitura++;

                    System.out.println(
                            "ERRO: " + ficheiro.getFileName()
                                    + ", linha " + linhaNumero
                                    + ": " + e.getMessage());
                }
            }

            System.out.println(
                    "p" + p + " / no " + no
                            + ": " + quantidade + " registos");
        }

        // ---------------------------------------------------------
        // 2. CONTINUIDADE DAS SEQUENCIAS
        // ---------------------------------------------------------

        System.out.println();
        System.out.println("=== 2. CONTINUIDADE DAS SEQUENCIAS ===");

        for (int p = 1; p <= PARTITIONS; p++) {

            Map<Integer, TreeSet<Long>> nos =
                    sequencias.getOrDefault(p, new TreeMap<>());

            for (int no = 1; no <= GROUP_SIZE; no++) {

                TreeSet<Long> seqs = nos.get(no);

                if (seqs == null || seqs.isEmpty()) {
                    System.out.println(
                            "AVISO: p" + p + " / no " + no
                                    + " sem registos.");
                    continue;
                }

                long esperada = 1;
                long faltasNo = 0;

                for (long seq : seqs) {
                    if (seq > esperada) {
                        faltasNo += seq - esperada;
                    }

                    esperada = Math.max(esperada, seq + 1);
                }

                lacunas += faltasNo;

                System.out.println(
                        "p" + p + " / no " + no
                                + ": primeiro=" + seqs.first()
                                + ", ultimo=" + seqs.last()
                                + ", sequencias=" + seqs.size()
                                + ", lacunas=" + faltasNo);
            }
        }

        // ---------------------------------------------------------
        // 3. EVENTOS
        // ---------------------------------------------------------

        System.out.println();
        System.out.println("=== 3. EVENTOS DE ELEICAO E LIDERANCA ===");

        for (Path ficheiro : ficheirosEventos) {

            Matcher matcher = EVENT_FILE.matcher(
                    ficheiro.getFileName().toString());

            if (!matcher.matches()) {
                continue;
            }

            int p = Integer.parseInt(matcher.group(1));
            int no = Integer.parseInt(matcher.group(2));

            if (!particaoValida(p) || !noValido(no)) {
                continue;
            }

            Estatisticas e = estatisticas.computeIfAbsent(
                    p, ignored -> new Estatisticas());

            for (String linha : Files.readAllLines(
                    ficheiro, StandardCharsets.UTF_8)) {

                if (linha.isBlank()) {
                    continue;
                }

                String[] f = linha.split(";", 4);

                if (f.length < 2) {
                    e.outros++;
                    continue;
                }

                long timestamp;

                try {
                    timestamp = Long.parseLong(f[0].trim());
                } catch (NumberFormatException ex) {
                    e.outros++;
                    continue;
                }

                String evento = f[1].trim();
                String detalhe = f.length >= 4 ? f[3].trim() : "";

                if (evento.equals("ESTADO")) {
                    continue;
                }

                Evento ev = new Evento(timestamp, p, no, evento, detalhe);
                todosEventos.add(ev);

                switch (evento) {
                    case "CANDIDATO" -> {
                        e.candidatos++;

                        if (ev.term >= 0) {
                            Eleicao el = obterEleicao(eleicoes, p, ev.term);
                            el.candidatos.add(no);
                            el.primeiroCandidato = Math.min(
                                    el.primeiroCandidato, timestamp);
                        }
                    }

                    case "VOTO" -> {
                        e.votos++;

                        if (ev.term >= 0) {
                            int candidato = extrairNumero(
                                    detalhe, "candidato");

                            if (candidato > 0) {
                                Eleicao el = obterEleicao(eleicoes, p, ev.term);

                                if (detalhe.toLowerCase(Locale.ROOT)
                                        .contains("concedido")) {
                                    el.votosConcedidos
                                            .computeIfAbsent(
                                                    candidato,
                                                    ignored -> new TreeSet<>())
                                            .add(no);
                                } else if (detalhe.toLowerCase(Locale.ROOT)
                                        .contains("recusado")) {
                                    el.votosRecusados
                                            .computeIfAbsent(
                                                    candidato,
                                                    ignored -> new TreeSet<>())
                                            .add(no);
                                }
                            }
                        }
                    }

                    case "LIDER" -> {
                        e.lideres++;

                        if (ev.term >= 0) {
                            Eleicao el = obterEleicao(eleicoes, p, ev.term);
                            int lider = extrairNumero(detalhe, "nó");

                            if (lider < 1) {
                                lider = extrairNumero(detalhe, "lider");
                            }
                            if (lider < 1) {
                                lider = no;
                            }

                            el.lideres.add(lider);
                            el.primeiroLider = Math.min(
                                    el.primeiroLider, timestamp);
                        }
                    }

                    case "DEMISSAO" -> e.demissoes++;
                    case "TERMO" -> e.termos++;
                    case "NOVO_LIDER" -> e.novosLideres++;
                    case "LOTE", "LOTE_PUBLICADO" -> e.lotes++;
                    case "RECEBIDO" -> e.recebidos++;

                    case "TRUNCADO" -> {
                        e.truncados++;
                        e.descartados += extrairNumero(
                                detalhe, "descartados");
                    }

                    case "ERRO", "ERRO_ELEICAO",
                         "ERRO_MENSAGEM", "LOTE_INVALIDO",
                         "ERRO_ENVIO_LOTE",
                         "RECUPERACAO_DIVERGENCIA_FALHOU",
                         "DIVERGENCIA_FINAL" -> e.erros++;

                    case "SUSPEITA" -> e.suspeitas++;

                    case "RETOMA", "REINICIO" -> e.recuperacoes++;

                    default -> e.outros++;
                }
            }
        }

        // ---------------------------------------------------------
        // 4. RESULTADOS POR TERMO E SEGURANCA
        // ---------------------------------------------------------

        System.out.println();
        System.out.println("=== 4. RESULTADOS DAS ELEICOES ===");

        long violacoesEleicao = 0;

        for (int p = 1; p <= PARTITIONS; p++) {

            System.out.println("Particao p" + p + ":");

            Map<Long, Eleicao> porTermo =
                    eleicoes.getOrDefault(p, new TreeMap<>());

            if (porTermo.isEmpty()) {
                System.out.println("  Sem eventos de eleicao identificados.");
            }

            for (Map.Entry<Long, Eleicao> entrada : porTermo.entrySet()) {

                long term = entrada.getKey();
                Eleicao el = entrada.getValue();

                System.out.println("  Termo " + term + ":");
                System.out.println("    Candidatos: " + el.candidatos);
                System.out.println("    Lideres: " + el.lideres);

                if (el.lideres.size() > 1) {
                    violacoesEleicao++;

                    System.out.println(
                            "    VIOLACAO: mais de um lider no mesmo termo!");
                } else if (el.lideres.size() == 1) {
                    System.out.println(
                            "    Resultado: eleito "
                                    + el.lideres.iterator().next());
                } else {
                    System.out.println(
                            "    Resultado: sem lider identificado / eleicao inconclusiva");
                }

                for (Map.Entry<Integer, Set<Integer>> votos
                        : el.votosConcedidos.entrySet()) {

                    System.out.println(
                            "    Votos concedidos ao candidato "
                                    + votos.getKey() + ": " + votos.getValue());
                }

                for (Map.Entry<Integer, Set<Integer>> votos
                        : el.votosRecusados.entrySet()) {

                    System.out.println(
                            "    Votos recusados ao candidato "
                                    + votos.getKey() + ": " + votos.getValue());
                }

                if (el.primeiroCandidato != Long.MAX_VALUE
                        && el.primeiroLider != Long.MAX_VALUE
                        && el.primeiroLider >= el.primeiroCandidato) {

                    System.out.printf(
                            Locale.US,
                            "    Duracao ate lider: %.3f segundos%n",
                            (el.primeiroLider - el.primeiroCandidato) / 1000.0);
                } else if (el.primeiroCandidato != Long.MAX_VALUE) {
                    System.out.println(
                            "    Duracao: nao foi identificado lider.");
                }
            }
        }

        System.out.println();
        System.out.println("Seguranca das eleicoes:");

        if (violacoesEleicao == 0) {
            System.out.println(
                    "  OK: nao foram identificados dois lideres no mesmo termo.");
        } else {
            System.out.println(
                    "  FALHA: " + violacoesEleicao
                            + " termos com mais de um lider.");
        }

        // ---------------------------------------------------------
        // 5. TEMPO ATE AO PRIMEIRO LIDER
        // ---------------------------------------------------------

        System.out.println();
        System.out.println("=== 5. TEMPO ATE AO PRIMEIRO LIDER ===");

        for (int p = 1; p <= PARTITIONS; p++) {

            long primeiroCandidato = Long.MAX_VALUE;
            long primeiroLider = Long.MAX_VALUE;

            for (Evento ev : todosEventos) {
                if (ev.partition != p) {
                    continue;
                }

                if (ev.tipo.equals("CANDIDATO")) {
                    primeiroCandidato = Math.min(
                            primeiroCandidato, ev.timestamp);
                }

                if (ev.tipo.equals("LIDER")) {
                    primeiroLider = Math.min(
                            primeiroLider, ev.timestamp);
                }
            }

            if (primeiroLider == Long.MAX_VALUE) {
                System.out.println("p" + p + ": sem lider identificado.");
            } else if (primeiroCandidato != Long.MAX_VALUE) {
                System.out.printf(
                        Locale.US,
                        "p%d: %.3f segundos desde o primeiro candidato.%n",
                        p, (primeiroLider - primeiroCandidato) / 1000.0);
            } else {
                System.out.println(
                        "p" + p + ": lider identificado, sem candidato registado.");
            }
        }

        // ---------------------------------------------------------
        // 6. MONOTONICIDADE DOS TERMOS
        // ---------------------------------------------------------

        System.out.println();
        System.out.println("=== 6. MONOTONICIDADE DOS TERMOS ===");

        long violacoesTermo = verificarMonotonicidade(todosEventos);

        if (violacoesTermo == 0) {
            System.out.println(
                    "OK: nao foram identificadas diminuicoes de termo "
                            + "nos eventos eleitorais locais.");
        } else {
            System.out.println(
                    "FALHA: " + violacoesTermo
                            + " diminuicoes de termo identificadas.");
        }

        // ---------------------------------------------------------
        // 7. HEARTBEATS
        // ---------------------------------------------------------

        System.out.println();
        System.out.println("=== 7. FICHEIROS DE HEARTBEAT ===");

        long heartbeatsLidos = 0;
        long errosHeartbeat = 0;

        for (Path ficheiro : ficheirosHeartbeat) {

            Matcher matcher = HB_FILE.matcher(
                    ficheiro.getFileName().toString());

            if (!matcher.matches()) {
                continue;
            }

            int p = Integer.parseInt(matcher.group(1));
            int no = Integer.parseInt(matcher.group(2));

            long quantidade = 0;
            long ultimoHeartbeat = -1;
            long ultimoTerm = -1;

            for (String linha : Files.readAllLines(
                    ficheiro, StandardCharsets.UTF_8)) {

                if (linha.isBlank()) {
                    continue;
                }

                try {
                    Heartbeat hb = new Heartbeat(linha);

                    quantidade++;
                    heartbeatsLidos++;

                    if (hb.term < ultimoTerm) {
                        System.out.println(
                                "VIOLACAO: termo diminuiu em heartbeat p"
                                        + p + "/n" + no);
                    }

                    if (hb.term == ultimoTerm
                            && ultimoHeartbeat >= 0
                            && hb.heartbeat < ultimoHeartbeat) {
                        System.out.println(
                                "AVISO: heartbeat diminuiu em p"
                                        + p + "/n" + no);
                    }

                    ultimoTerm = hb.term;
                    ultimoHeartbeat = hb.heartbeat;

                } catch (Exception ex) {
                    errosHeartbeat++;
                    System.out.println(
                            "ERRO heartbeat em " + ficheiro.getFileName()
                                    + ": " + ex.getMessage());
                }
            }

            System.out.println(
                    "p" + p + " / no " + no
                            + ": " + quantidade + " heartbeats");
        }

        // ---------------------------------------------------------
        // 8. TRUNCAMENTOS
        // ---------------------------------------------------------

        System.out.println();
        System.out.println("=== 8. RECUPERACAO DE DIVERGENCIAS ===");

        long truncamentos = 0;
        long registosDescartados = 0;

        for (Estatisticas e : estatisticas.values()) {
            truncamentos += e.truncados;
            registosDescartados += e.descartados;
        }

        System.out.println("Eventos TRUNCADO: " + truncamentos);
        System.out.println(
                "Registos descartados segundo os eventos: "
                        + registosDescartados);

        // ---------------------------------------------------------
        // 9. COINCIDENCIA FINAL DAS REPLICAS
        // ---------------------------------------------------------

        System.out.println();
        System.out.println("=== 9. COINCIDENCIA FINAL DAS REPLICAS ===");

        for (int p = 1; p <= PARTITIONS; p++) {

            Map<Integer, TreeMap<Long, Registo>> nos =
                    dados.getOrDefault(p, new TreeMap<>());

            long menorUltimaSeq = Long.MAX_VALUE;
            boolean existemNos = false;

            for (int no = 1; no <= GROUP_SIZE; no++) {
                TreeMap<Long, Registo> registos = nos.get(no);

                if (registos != null && !registos.isEmpty()) {
                    existemNos = true;
                    menorUltimaSeq = Math.min(
                            menorUltimaSeq, registos.lastKey());
                }
            }

            if (!existemNos) {
                System.out.println("p" + p + ": sem dados para comparar.");
                continue;
            }

            long comparados = 0;
            long diferentes = 0;

            for (long seq = 1; seq <= menorUltimaSeq; seq++) {

                String referencia = null;
                boolean primeiro = true;

                for (int no = 1; no <= GROUP_SIZE; no++) {

                    TreeMap<Long, Registo> registos = nos.get(no);

                    if (registos == null) {
                        continue;
                    }

                    Registo r = registos.get(seq);

                    if (r == null) {
                        diferentes++;
                        continue;
                    }

                    String conteudo = r.term + ";" + r.conteudo();

                    if (primeiro) {
                        referencia = conteudo;
                        primeiro = false;
                    } else if (!referencia.equals(conteudo)) {
                        diferentes++;
                    }
                }

                comparados++;
            }

            System.out.println(
                    "p" + p + ": comparadas " + comparados
                            + " sequencias ate seq=" + menorUltimaSeq
                            + "; diferencas=" + diferentes);

            if (diferentes == 0) {
                System.out.println(
                        "  OK: replicas coincidem no intervalo comparado.");
            } else {
                System.out.println(
                        "  FALHA: replicas apresentam diferencas.");
            }
        }

        // ---------------------------------------------------------
        // 10. LEITURAS DO PRODUTOR
        // ---------------------------------------------------------

        System.out.println();
        System.out.println("=== 10. LEITURAS DO PRODUTOR ===");

        Path pastaProjeto = pasta.toAbsolutePath().normalize().getParent();

        Path produtorFile = pastaProjeto
                .resolve("raft04-produtor")
                .resolve("produtor.txt");

        if (!Files.exists(produtorFile)) {
            try (var caminhos = Files.walk(pastaProjeto, 5)) {
                produtorFile = caminhos
                        .filter(Files::isRegularFile)
                        .filter(p -> p.getFileName().toString()
                                .equalsIgnoreCase("produtor.txt"))
                        .findFirst()
                        .orElse(pasta.resolve("produtor.txt"));
            } catch (IOException e) {
                System.out.println(
                        "Erro ao procurar produtor.txt: " + e.getMessage());
            }
        }

        long leiturasEnviadas = 0;
        long leiturasEncontradas = 0;
        long leiturasPerdidas = 0;
        long leiturasAntigasIgnoradas = 0;
        long errosProdutor = 0;

        if (!Files.exists(produtorFile)) {

            System.out.println("AVISO: produtor.txt nao encontrado.");

        } else {

            long primeiroRegisto = Long.MAX_VALUE;

            for (Registo r : registosUnicos.values()) {
                primeiroRegisto = Math.min(
                        primeiroRegisto, r.instante);
            }

            long limiteInferior;

            if (primeiroRegisto == Long.MAX_VALUE) {
                limiteInferior = Long.MIN_VALUE;
            } else {
                limiteInferior = Math.max(
                        0, primeiroRegisto - MARGEM_EXECUCAO_MS);
            }

            System.out.println(
                    "Inicio aproximado da execucao: "
                            + (primeiroRegisto == Long.MAX_VALUE
                            ? "nao determinado"
                            : primeiroRegisto));

            Map<String, Long> enviadas = new HashMap<>();

            for (String linha : Files.readAllLines(
                    produtorFile, StandardCharsets.UTF_8)) {

                if (linha.isBlank()) {
                    continue;
                }

                try {
                    LeituraProdutor leitura =
                            new LeituraProdutor(linha);

                    if (leitura.instante < limiteInferior) {
                        leiturasAntigasIgnoradas++;
                        continue;
                    }

                    enviadas.merge(
                            leitura.chave(), 1L, Long::sum);

                    leiturasEnviadas++;

                } catch (Exception e) {
                    errosProdutor++;

                    System.out.println(
                            "ERRO em produtor.txt: " + e.getMessage());
                }
            }

            Map<String, Long> armazenadas = new HashMap<>();

            for (Registo r : registosUnicos.values()) {
                armazenadas.merge(
                        r.leitura(), 1L, Long::sum);
            }

            for (Map.Entry<String, Long> entrada : enviadas.entrySet()) {

                long enviadasQuantidade = entrada.getValue();

                long armazenadasQuantidade =
                        armazenadas.getOrDefault(entrada.getKey(), 0L);

                long encontradas = Math.min(
                        enviadasQuantidade, armazenadasQuantidade);

                leiturasEncontradas += encontradas;
                leiturasPerdidas += enviadasQuantidade - encontradas;
            }

            System.out.println(
                    "Leituras publicadas nesta execucao: "
                            + leiturasEnviadas);

            System.out.println(
                    "Leituras encontradas nos nos: "
                            + leiturasEncontradas);

            System.out.println(
                    "Leituras nao encontradas: "
                            + leiturasPerdidas);

            System.out.println(
                    "Leituras antigas ignoradas: "
                            + leiturasAntigasIgnoradas);

            System.out.println(
                    "Erros em produtor.txt: "
                            + errosProdutor);
        }

        // ---------------------------------------------------------
        // 11. FICHEIRO DE FALHAS
        // ---------------------------------------------------------

        System.out.println();
        System.out.println("=== 11. FICHEIRO DE FALHAS ===");

        Path falhasFile = falhasArgumento != null
                ? falhasArgumento
                : pasta.resolve("falhas.txt");

        if (Files.exists(falhasFile)) {

            List<String> linhas = Files.readAllLines(
                    falhasFile, StandardCharsets.UTF_8);

            System.out.println(
                    "Ficheiro de falhas encontrado: " + falhasFile);
            System.out.println("Linhas: " + linhas.size());

            for (String linha : linhas) {
                if (!linha.isBlank()) {
                    System.out.println("  " + linha);
                }
            }

            System.out.println(
                    "NOTA: os intervalos de indisponibilidade devem ser "
                            + "correlacionados com os eventos e leituras "
                            + "do produtor do ensaio.");

        } else {
            System.out.println(
                    "falhas.txt nao encontrado. Analise de falhas omitida.");
        }

        // ---------------------------------------------------------
        // 12. ESTATISTICAS POR PARTICAO
        // ---------------------------------------------------------

        System.out.println();
        System.out.println("=== 12. RESUMO DOS EVENTOS ===");

        for (int p = 1; p <= PARTITIONS; p++) {

            Estatisticas e = estatisticas.get(p);

            if (e == null) {
                System.out.println("p" + p + ": sem ficheiro de eventos.");
                continue;
            }

            System.out.println("Particao p" + p + ":");
            System.out.println("  Candidatos: " + e.candidatos);
            System.out.println("  Votos: " + e.votos);
            System.out.println("  Eventos LIDER: " + e.lideres);
            System.out.println("  Demissoes: " + e.demissoes);
            System.out.println("  Alteracoes de termo: " + e.termos);
            System.out.println("  Novos lideres observados: " + e.novosLideres);
            System.out.println("  Lotes publicados: " + e.lotes);
            System.out.println("  Lotes recebidos: " + e.recebidos);
            System.out.println("  TRUNCADO: " + e.truncados);
            System.out.println("  Registos descartados: " + e.descartados);
            System.out.println("  Erros: " + e.erros);
            System.out.println("  Suspeitas: " + e.suspeitas);
            System.out.println("  Recuperacoes/reinicios: " + e.recuperacoes);
        }

        // ---------------------------------------------------------
        // 13. RESUMO FINAL
        // ---------------------------------------------------------

        System.out.println();
        System.out.println("==================================");
        System.out.println("          RESUMO FINAL");
        System.out.println("==================================");

        System.out.println(
                "Ficheiros de nos analisados: " + ficheirosNos.size());

        System.out.println(
                "Ficheiros de eventos analisados: " + ficheirosEventos.size());

        System.out.println(
                "Ficheiros heartbeat analisados: " + ficheirosHeartbeat.size());

        System.out.println(
                "Registos lidos (incluindo replicas): " + registosLidos);

        System.out.println(
                "Registos logicos unicos: " + registosUnicos.size());

        System.out.println(
                "Copias repetidas entre replicas: " + duplicados);

        System.out.println(
                "Divergencias entre replicas: " + divergencias);

        System.out.println("Lacunas nas sequencias: " + lacunas);
        System.out.println("Erros de leitura dos registos: " + errosLeitura);

        System.out.println("Heartbeats lidos: " + heartbeatsLidos);
        System.out.println("Erros nos heartbeats: " + errosHeartbeat);

        System.out.println("Violacoes de seguranca de eleicao: "
                + violacoesEleicao);

        System.out.println("Violacoes de monotonicidade: "
                + violacoesTermo);

        System.out.println("Eventos TRUNCADO: " + truncamentos);
        System.out.println("Registos descartados: " + registosDescartados);

        if (Files.exists(produtorFile)) {

            System.out.println(
                    "Leituras enviadas nesta execucao: " + leiturasEnviadas);

            System.out.println(
                    "Leituras encontradas: " + leiturasEncontradas);

            System.out.println(
                    "Leituras nao encontradas: " + leiturasPerdidas);

            System.out.println(
                    "Leituras antigas ignoradas: "
                            + leiturasAntigasIgnoradas);
        }

        System.out.println();
        System.out.println(
                "NOTA: uma leitura nao encontrada nos ficheiros dos nos "
                        + "nao prova, por si so, perda definitiva. "
                        + "Confirma os resultados com os eventos e os ensaios.");

        System.out.println("Analise terminada.");
    }

    // -------------------------------------------------------------
    // METODOS AUXILIARES
    // -------------------------------------------------------------

    private static String chaveNo(int p, int no) {
        return "p" + p + ";n" + no;
    }

    private static boolean particaoValida(int p) {
        return p >= 1 && p <= PARTITIONS;
    }

    private static boolean noValido(int no) {
        return no >= 1 && no <= GROUP_SIZE;
    }

    private static long extrairTermo(String detalhe) {
        Matcher matcher = TERM_PATTERN.matcher(detalhe);

        if (matcher.find()) {
            try {
                return Long.parseLong(matcher.group(1));
            } catch (NumberFormatException ignored) {
                return -1;
            }
        }

        return -1;
    }

    private static int extrairNumero(String texto, String campo) {

        Pattern pattern = Pattern.compile(
                Pattern.quote(campo) + "\\s*=\\s*(?:n)?([0-9]+)",
                Pattern.CASE_INSENSITIVE);

        Matcher matcher = pattern.matcher(texto);

        if (matcher.find()) {
            try {
                return Integer.parseInt(matcher.group(1));
            } catch (NumberFormatException ignored) {
                return -1;
            }
        }

        return -1;
    }

    private static Eleicao obterEleicao(
            Map<Integer, Map<Long, Eleicao>> mapa,
            int p,
            long term) {

        mapa.putIfAbsent(p, new TreeMap<>());

        return mapa.get(p).computeIfAbsent(
                term, ignored -> new Eleicao());
    }

    /**
     * Verifica a monotonicidade apenas nos eventos que representam
     * o estado eleitoral local do nó.
     *
     * Eventos como RECEBIDO e LOTE_PUBLICADO podem transportar
     * termos de outros nós ou de mensagens antigas, pelo que não
     * devem ser usados para validar o termo persistente local.
     */
    private static long verificarMonotonicidade(List<Evento> eventos) {

        Map<String, List<Evento>> porNo = new TreeMap<>();

        for (Evento ev : eventos) {

            if (ev.term < 0) {
                continue;
            }

            boolean eventoLocal = switch (ev.tipo) {
                case "CANDIDATO", "VOTO", "LIDER",
                     "TERMO", "DEMISSAO" -> true;
                default -> false;
            };

            if (!eventoLocal) {
                continue;
            }

            String chave = chaveNo(ev.partition, ev.node);

            porNo.computeIfAbsent(
                    chave, ignored -> new ArrayList<>()).add(ev);
        }

        long violacoes = 0;

        for (Map.Entry<String, List<Evento>> entrada : porNo.entrySet()) {

            List<Evento> lista = entrada.getValue();

            lista.sort(Comparator.comparingLong(ev -> ev.timestamp));

            long anterior = -1;

            for (Evento ev : lista) {

                if (anterior >= 0 && ev.term < anterior) {
                    violacoes++;

                    System.out.println(
                            "VIOLACAO: " + entrada.getKey()
                                    + " passou de termo " + anterior
                                    + " para " + ev.term
                                    + " no evento " + ev.tipo
                                    + " (timestamp=" + ev.timestamp + ")");
                }

                anterior = Math.max(anterior, ev.term);
            }
        }

        return violacoes;
    }

    private static List<Path> listar(Path pasta, Pattern pattern)
            throws IOException {

        List<Path> resultado = new ArrayList<>();

        try (var paths = Files.list(pasta)) {
            paths.filter(Files::isRegularFile)
                    .filter(p -> pattern.matcher(
                            p.getFileName().toString()).matches())
                    .sorted()
                    .forEach(resultado::add);
        }

        return resultado;
    }
}
