package rep04;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

// Analise a posteriori do detetor de falhas.
//
// Uso: HeartbeatCheck <N> <pasta> [falhas.txt]
//
// Le hb-p<k>.txt, lider-p<k>.txt e monitor-*.txt da pasta. Os instantes dos
// varios ficheiros sao comparados entre si, pelo que so fazem sentido se
// todos os processos correram na mesma maquina.
public class HeartbeatCheck {

    private static final List<String> RELATORIO = new ArrayList<>();

    // Um heartbeat publicado: inc;hb;instante;ultimoSeq;n;bytes
    private static class Hb {
        long inc;
        long hb;
        long instante;
        long ultimoSeq;
        int n;
        int bytes;
    }

    // Um evento do detetor: instante;evento;p<k>;detalhe
    private static class Ev {
        long t;
        String tipo;
        int particao;
        String detalhe;
    }

    // Um registo do lider (so interessam seq, instante e a linha)
    private static class Reg {
        long seq;
        long instante;   // -1 se o ficheiro nao tiver instante
        String linha;
    }

    // Uma falha provocada: p<k>;instante
    private static class Falha {
        int particao;
        long t;
    }

    private static long base = 0;

    public static void main(String[] args) {

        if (args.length < 2 || args.length > 3) {
            System.out.println("Uso: HeartbeatCheck <N> <pasta> [falhas.txt]");
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

            Path falhas = null;

            if (args.length == 3) {
                falhas = Path.of(args[2]);

                if (!Files.isRegularFile(falhas)) {
                    Path alternativa = pasta.resolve(args[2]);

                    if (!Files.isRegularFile(alternativa)) {
                        throw new IOException(
                                "Ficheiro de falhas nao encontrado: " + args[2]);
                    }

                    falhas = alternativa;
                }
            }

            executar(n, pasta, falhas);

        } catch (Exception e) {
            System.err.println("Erro: " + e.getMessage());
        }
    }

    private static void linha(String s) {
        System.out.println(s);
        RELATORIO.add(s);
    }

    private static void linha(String formato, Object... args) {
        linha(String.format(Locale.US, formato, args));
    }

    // Instante relativo ao primeiro heartbeat publicado
    private static String rel(long t) {
        return String.format(Locale.US, "%+.1f s", (t - base) / 1000.0);
    }

    private static void executar(int n, Path pasta, Path ficheiroFalhas)
            throws IOException {

        List<List<Hb>> hbs = new ArrayList<>();
        List<List<Reg>> registos = new ArrayList<>();

        for (int k = 0; k < n; k++) {
            hbs.add(lerHb(pasta.resolve("hb-p" + k + ".txt")));
            registos.add(lerRegistos(pasta.resolve("lider-p" + k + ".txt")));
        }

        Map<String, List<Ev>> monitores = lerMonitores(pasta);
        List<Falha> falhas = ficheiroFalhas == null
                ? new ArrayList<>() : lerFalhas(ficheiroFalhas);

        long menor = Long.MAX_VALUE;

        for (List<Hb> l : hbs) {
            for (Hb h : l) {
                menor = Math.min(menor, h.instante);
            }
        }

        base = menor == Long.MAX_VALUE ? 0 : menor;

        linha("=== HEARTBEAT CHECK ===");
        linha("Particoes: %d", n);
        linha("Pasta: %s", pasta.toAbsolutePath());
        linha("Participantes: %s", monitores.keySet());
        linha("Instantes relativos ao primeiro heartbeat publicado (t=%d)", base);

        secaoLotes(n, hbs);
        secaoSuspeitas(n, hbs, monitores);

        if (ficheiroFalhas != null) {
            secaoDeteccao(n, monitores, falhas);
        }

        secaoLatencia(n, registos, monitores);
        secaoEventos(monitores);
        secaoBytes(n, hbs, registos);

        Path destino = pasta.resolve("heartbeat-relatorio.txt");
        Files.write(destino, RELATORIO, StandardCharsets.UTF_8);

        System.out.println();
        System.out.println("Relatorio gravado em: " + destino.toAbsolutePath());
    }

    // ---------------------------------------------------------------- leitura

    private static List<Hb> lerHb(Path ficheiro) throws IOException {
        List<Hb> lista = new ArrayList<>();

        if (!Files.exists(ficheiro)) {
            System.out.println("AVISO: ficheiro nao encontrado: "
                    + ficheiro.getFileName());
            return lista;
        }

        int numero = 0;

        for (String l : Files.readAllLines(ficheiro, StandardCharsets.UTF_8)) {
            numero++;

            if (l.isBlank()) {
                continue;
            }

            try {
                String[] f = l.trim().split(";", -1);

                if (f.length != 6) {
                    throw new IllegalArgumentException("esperados 6 campos");
                }

                Hb h = new Hb();
                h.inc = Long.parseLong(f[0].trim());
                h.hb = Long.parseLong(f[1].trim());
                h.instante = Long.parseLong(f[2].trim());
                h.ultimoSeq = Long.parseLong(f[3].trim());
                h.n = Integer.parseInt(f[4].trim());
                h.bytes = Integer.parseInt(f[5].trim());
                lista.add(h);

            } catch (IllegalArgumentException e) {
                System.out.println("AVISO: " + ficheiro.getFileName()
                        + " linha " + numero + " ignorada: " + e.getMessage());
            }
        }

        lista.sort(Comparator.<Hb>comparingLong(h -> h.inc)
                .thenComparingLong(h -> h.hb));

        return lista;
    }

    private static List<Reg> lerRegistos(Path ficheiro) throws IOException {
        List<Reg> lista = new ArrayList<>();

        if (!Files.exists(ficheiro)) {
            System.out.println("AVISO: ficheiro nao encontrado: "
                    + ficheiro.getFileName());
            return lista;
        }

        int numero = 0;

        for (String l : Files.readAllLines(ficheiro, StandardCharsets.UTF_8)) {
            numero++;

            if (l.isBlank()) {
                continue;
            }

            try {
                String[] f = l.trim().split(";", -1);

                if (f.length != 6) {
                    throw new IllegalArgumentException("esperados 6 campos");
                }

                Reg r = new Reg();
                r.linha = l.trim();

                if (f[1].trim().matches("\\d+")) {
                    // formato antigo: particao;seq;sensor;temperatura;L;V
                    r.seq = Long.parseLong(f[1].trim());
                    r.instante = -1;
                } else {
                    // seq;sensor;temperatura;instante;L;V
                    r.seq = Long.parseLong(f[0].trim());
                    r.instante = Long.parseLong(f[3].trim());
                }

                lista.add(r);

            } catch (IllegalArgumentException e) {
                System.out.println("AVISO: " + ficheiro.getFileName()
                        + " linha " + numero + " ignorada: " + e.getMessage());
            }
        }

        return lista;
    }

    private static Map<String, List<Ev>> lerMonitores(Path pasta)
            throws IOException {

        Map<String, List<Ev>> monitores = new TreeMap<>();

        try (DirectoryStream<Path> ds =
                     Files.newDirectoryStream(pasta, "monitor-*.txt")) {

            for (Path ficheiro : ds) {
                String nome = ficheiro.getFileName().toString();
                String id = nome.substring(
                        "monitor-".length(), nome.length() - ".txt".length());

                List<Ev> eventos = new ArrayList<>();
                int numero = 0;

                for (String l : Files.readAllLines(
                        ficheiro, StandardCharsets.UTF_8)) {
                    numero++;

                    if (l.isBlank()) {
                        continue;
                    }

                    try {
                        String[] f = l.trim().split(";", 4);

                        if (f.length < 3 || !f[2].startsWith("p")) {
                            throw new IllegalArgumentException(
                                    "esperado instante;evento;p<k>;detalhe");
                        }

                        Ev e = new Ev();
                        e.t = Long.parseLong(f[0].trim());
                        e.tipo = f[1].trim();
                        e.particao = Integer.parseInt(f[2].substring(1));
                        e.detalhe = f.length > 3 ? f[3] : "";
                        eventos.add(e);

                    } catch (IllegalArgumentException e) {
                        System.out.println("AVISO: " + nome + " linha "
                                + numero + " ignorada: " + e.getMessage());
                    }
                }

                // estavel: eventos com o mesmo instante mantem a ordem do ficheiro
                eventos.sort(Comparator.comparingLong((Ev e) -> e.t));
                monitores.put(id, eventos);
            }
        }

        return monitores;
    }

    private static List<Falha> lerFalhas(Path ficheiro) throws IOException {
        List<Falha> lista = new ArrayList<>();
        Pattern padrao = Pattern.compile("^p(\\d+);(\\d+)$");
        int numero = 0;

        for (String l : Files.readAllLines(ficheiro, StandardCharsets.UTF_8)) {
            numero++;

            if (l.isBlank()) {
                continue;
            }

            Matcher m = padrao.matcher(l.trim());

            if (!m.matches()) {
                System.out.println("AVISO: " + ficheiro.getFileName()
                        + " linha " + numero + " ignorada: esperado p<k>;instante");
                continue;
            }

            Falha f = new Falha();
            f.particao = Integer.parseInt(m.group(1));
            f.t = Long.parseLong(m.group(2));
            lista.add(f);
        }

        lista.sort(Comparator.comparingLong((Falha f) -> f.t));
        return lista;
    }

    // Valor de chave=valor num detalhe com campos separados por ';'
    private static String campo(String detalhe, String chave) {
        for (String t : detalhe.split(";")) {
            if (t.startsWith(chave + "=")) {
                return t.substring(chave.length() + 1);
            }
        }
        return null;
    }

    private static long campoLong(String detalhe, String chave, long omissao) {
        String v = campo(detalhe, chave);

        if (v == null) {
            return omissao;
        }

        try {
            return Long.parseLong(v.trim());
        } catch (NumberFormatException e) {
            return omissao;
        }
    }

    // ------------------------------------------------------------ 1. lotes

    private static void secaoLotes(int n, List<List<Hb>> hbs) {
        linha("");
        linha("--- 1. Lotes por particao e incarnacao ---");

        for (int k = 0; k < n; k++) {
            List<Hb> lista = hbs.get(k);

            if (lista.isEmpty()) {
                linha("Particao %d: sem heartbeats publicados", k);
                continue;
            }

            Map<Long, List<Hb>> porInc = new TreeMap<>();

            for (Hb h : lista) {
                porInc.computeIfAbsent(h.inc, x -> new ArrayList<>()).add(h);
            }

            linha("Particao %d: %d lotes em %d incarnacao(oes)",
                    k, lista.size(), porInc.size());

            for (Map.Entry<Long, List<Hb>> e : porInc.entrySet()) {
                List<Hb> g = e.getValue();

                int vazios = 0;
                long somaN = 0;
                int maxN = 0;
                long somaIntervalo = 0;
                long maxIntervalo = 0;
                long emFalta = g.get(0).hb - 1;

                for (int i = 0; i < g.size(); i++) {
                    Hb h = g.get(i);

                    if (h.n == 0) {
                        vazios++;
                    }

                    somaN += h.n;
                    maxN = Math.max(maxN, h.n);

                    if (i > 0) {
                        long intervalo = h.instante - g.get(i - 1).instante;
                        somaIntervalo += intervalo;
                        maxIntervalo = Math.max(maxIntervalo, intervalo);
                        emFalta += h.hb - g.get(i - 1).hb - 1;
                    }
                }

                linha("  inc=%d (%s): lotes=%d vazios=%d (%.1f%%) "
                                + "registos/lote media=%.2f max=%d",
                        e.getKey(), rel(g.get(0).instante), g.size(), vazios,
                        100.0 * vazios / g.size(), (double) somaN / g.size(), maxN);

                if (g.size() > 1) {
                    linha("      intervalo entre heartbeats: media=%.0f ms max=%d ms",
                            (double) somaIntervalo / (g.size() - 1), maxIntervalo);
                } else {
                    linha("      intervalo entre heartbeats: (so um heartbeat)");
                }

                if (emFalta > 0) {
                    linha("      hb em falta (publicacoes falhadas): %d", emFalta);
                }
            }
        }
    }

    // ---------------------------------------------------------- 2. suspeitas

    // Ultimo heartbeat publicado ate ao instante t (ou null)
    private static Hb ultimoHbAte(List<Hb> hbs, long t) {
        Hb melhor = null;

        for (Hb h : hbs) {
            if (h.instante <= t && (melhor == null
                    || h.instante > melhor.instante)) {
                melhor = h;
            }
        }

        return melhor;
    }

    private static void secaoSuspeitas(int n, List<List<Hb>> hbs,
                                       Map<String, List<Ev>> monitores) {
        linha("");
        linha("--- 2. Suspeitas por participante e particao ---");

        long totalReais = 0;
        long totalFalsas = 0;

        for (Map.Entry<String, List<Ev>> entrada : monitores.entrySet()) {
            String participante = entrada.getKey();

            for (int k = 0; k < n; k++) {
                List<Ev> evs = new ArrayList<>();

                for (Ev e : entrada.getValue()) {
                    if (e.particao == k) {
                        evs.add(e);
                    }
                }

                long reais = 0;
                long falsas = 0;
                long incAtual = -1;

                List<String> detalhes = new ArrayList<>();

                for (int i = 0; i < evs.size(); i++) {
                    Ev e = evs.get(i);

                    if (e.tipo.equals("RECEBIDO")) {
                        incAtual = campoLong(e.detalhe, "inc", incAtual);
                        continue;
                    }

                    if (!e.tipo.equals("SUSPEITA")) {
                        continue;
                    }

                    // incarnacao suspeita: a do ultimo heartbeat recebido; se
                    // nunca recebeu nenhum, a do ultimo publicado
                    Hb ultimoPublicado = ultimoHbAte(hbs.get(k), e.t);
                    long incSuspeita = incAtual;

                    if (incSuspeita == -1 && ultimoPublicado != null) {
                        incSuspeita = ultimoPublicado.inc;
                    }

                    // primeira RETOMA depois da suspeita
                    Ev retoma = null;

                    for (int j = i + 1; j < evs.size(); j++) {
                        if (evs.get(j).tipo.equals("RETOMA")) {
                            retoma = evs.get(j);
                            break;
                        }
                    }

                    boolean falsa = retoma != null && incSuspeita != -1
                            && campoLong(retoma.detalhe, "inc", -2) == incSuspeita;

                    if (falsa) {
                        falsas++;
                    } else {
                        reais++;
                    }

                    String silencioPublicado = ultimoPublicado == null
                            ? "n/d"
                            : (e.t - ultimoPublicado.instante) + " ms";

                    String texto = String.format(Locale.US,
                            "  SUSPEITA t=%d (%s): %s | inc=%s | silencio desde "
                                    + "o ultimo hb publicado=%s (detetor: %s ms)",
                            e.t, rel(e.t),
                            falsa ? "FALSA SUSPEITA" : "FALHA REAL",
                            incSuspeita == -1 ? "?" : String.valueOf(incSuspeita),
                            silencioPublicado,
                            campo(e.detalhe, "silencio_ms"));

                    if (falsa) {
                        texto += String.format(Locale.US,
                                " | RETOMA com o mesmo inc em t=%d (%d ms depois)",
                                retoma.t, retoma.t - e.t);
                    } else if (retoma != null) {
                        texto += String.format(Locale.US,
                                " | RETOMA com outra incarnacao (inc=%s) em t=%d",
                                campo(retoma.detalhe, "inc"), retoma.t);
                    } else {
                        texto += " | sem RETOMA";
                    }

                    detalhes.add(texto);
                }

                if (!detalhes.isEmpty()) {
                    linha("%s p%d: suspeitas=%d (falhas reais=%d, falsas=%d)",
                            participante, k, reais + falsas, reais, falsas);

                    for (String d : detalhes) {
                        linha(d);
                    }
                }

                totalReais += reais;
                totalFalsas += falsas;
            }
        }

        linha("Total: falhas reais=%d falsas suspeitas=%d", totalReais, totalFalsas);
    }

    // ------------------------------------------------------ 3. deteccao

    private static void secaoDeteccao(int n, Map<String, List<Ev>> monitores,
                                      List<Falha> falhas) {
        linha("");
        linha("--- 3. Tempo de deteccao das falhas provocadas ---");

        long delta = Wire.PERIODO_HB_MS;
        long timeout = delta * Wire.FATOR_TIMEOUT;
        long minimo = timeout - delta;
        long maximo = timeout + Wire.VERIFICACAO_MS;

        linha("Previsto (sem atraso de rede): ]%d, %d] ms com delta=%d ms, "
                        + "fator=%d, verificacao=%d ms",
                minimo, maximo, delta, Wire.FATOR_TIMEOUT, Wire.VERIFICACAO_MS);
        linha("(delta e fator vem das propriedades rep04.periodo e rep04.fator "
                + "deste processo)");

        if (falhas.isEmpty()) {
            linha("Sem falhas em falhas.txt.");
            return;
        }

        List<Long> tempos = new ArrayList<>();

        for (Falha f : falhas) {
            if (f.particao >= n) {
                linha("Falha em p%d ignorada (N=%d).", f.particao, n);
                continue;
            }

            // a janela da falha acaba na falha seguinte da mesma particao
            long fim = Long.MAX_VALUE;

            for (Falha g : falhas) {
                if (g.particao == f.particao && g.t > f.t) {
                    fim = Math.min(fim, g.t);
                }
            }

            linha("Falha p%d em t=%d (%s):", f.particao, f.t, rel(f.t));

            for (Map.Entry<String, List<Ev>> entrada : monitores.entrySet()) {
                Ev suspeita = null;
                boolean acompanha = false;

                for (Ev e : entrada.getValue()) {
                    if (e.particao == f.particao) {
                        acompanha = true;
                        break;
                    }
                }

                // participante que nao guarda esta particao (sem eventos)
                if (!acompanha) {
                    continue;
                }

                for (Ev e : entrada.getValue()) {
                    if (e.particao == f.particao && e.tipo.equals("SUSPEITA")
                            && e.t >= f.t && e.t < fim) {
                        suspeita = e;
                        break;
                    }
                }

                if (suspeita == null) {
                    linha("  %s: nao detetada", entrada.getKey());
                    continue;
                }

                long deteccao = suspeita.t - f.t;
                tempos.add(deteccao);

                linha("  %s: %d ms (%.2f s) - %s do intervalo previsto",
                        entrada.getKey(), deteccao, deteccao / 1000.0,
                        deteccao > minimo && deteccao <= maximo ? "dentro" : "FORA");
            }
        }

        if (tempos.isEmpty()) {
            linha("Nenhuma falha detetada.");
        } else {
            long min = Long.MAX_VALUE;
            long max = Long.MIN_VALUE;
            long soma = 0;

            for (long t : tempos) {
                min = Math.min(min, t);
                max = Math.max(max, t);
                soma += t;
            }

            linha("Tempo de deteccao (%d medicoes): minimo=%.2f s medio=%.2f s "
                            + "maximo=%.2f s",
                    tempos.size(), min / 1000.0,
                    soma / 1000.0 / tempos.size(), max / 1000.0);
        }
    }

    // ------------------------------------------------------- 4. latencia

    private static void secaoLatencia(int n, List<List<Reg>> registos,
                                      Map<String, List<Ev>> monitores) {
        linha("");
        linha("--- 4. Latencia de lote (RECEBIDO que contem o registo - instante do registo) ---");
        linha("Para cada registo usa-se o primeiro RECEBIDO entre os participantes que indicam o intervalo de seq.");

        for (int k = 0; k < n; k++) {
            Map<Long, Long> primeiro = new HashMap<>();

            for (List<Ev> evs : monitores.values()) {
                for (Ev e : evs) {
                    if (e.particao != k || !e.tipo.equals("RECEBIDO")) {
                        continue;
                    }

                    String seq = campo(e.detalhe, "seq");

                    if (seq == null || seq.equals("-")) {
                        continue;
                    }

                    String[] ab = seq.split("-");

                    if (ab.length != 2) {
                        continue;
                    }

                    try {
                        long a = Long.parseLong(ab[0].trim());
                        long b = Long.parseLong(ab[1].trim());

                        for (long s = a; s <= b; s++) {
                            primeiro.merge(s, e.t, Math::min);
                        }
                    } catch (NumberFormatException ex) {
                        // ignora
                    }
                }
            }

            long medidos = 0;
            long soma = 0;
            long max = Long.MIN_VALUE;
            long min = Long.MAX_VALUE;
            long semRecebido = 0;
            long semInstante = 0;

            for (Reg r : registos.get(k)) {
                if (r.instante < 0) {
                    semInstante++;
                    continue;
                }

                Long t = primeiro.get(r.seq);

                if (t == null) {
                    semRecebido++;
                    continue;
                }

                long latencia = t - r.instante;
                medidos++;
                soma += latencia;
                max = Math.max(max, latencia);
                min = Math.min(min, latencia);
            }

            if (medidos == 0) {
                linha("Particao %d: sem registos medidos (sem RECEBIDO=%d, "
                        + "sem instante=%d)", k, semRecebido, semInstante);
            } else {
                linha("Particao %d: registos medidos=%d media=%.0f ms max=%d ms "
                                + "min=%d ms | registos sem RECEBIDO=%d",
                        k, medidos, (double) soma / medidos, max, min, semRecebido);
            }
        }
    }

    // --------------------------------------------------------- 5. eventos

    private static void secaoEventos(Map<String, List<Ev>> monitores) {
        linha("");
        linha("--- 5. HB_PERDIDO e RECUPERACAO por participante ---");

        long totalPerdidos = 0;
        long totalRecuperacoes = 0;

        for (Map.Entry<String, List<Ev>> entrada : monitores.entrySet()) {
            long perdidos = 0;
            long recuperacoes = 0;
            long reinicios = 0;

            for (Ev e : entrada.getValue()) {
                switch (e.tipo) {
                    case "HB_PERDIDO":
                        perdidos++;
                        break;
                    case "RECUPERACAO":
                        recuperacoes++;
                        break;
                    case "REINICIO":
                        reinicios++;
                        break;
                    default:
                        break;
                }
            }

            linha("%s: HB_PERDIDO=%d RECUPERACAO=%d (REINICIO=%d)",
                    entrada.getKey(), perdidos, recuperacoes, reinicios);

            totalPerdidos += perdidos;
            totalRecuperacoes += recuperacoes;
        }

        linha("Total: HB_PERDIDO=%d RECUPERACAO=%d",
                totalPerdidos, totalRecuperacoes);
    }

    // ----------------------------------------------------------- 6. bytes

    private static void secaoBytes(int n, List<List<Hb>> hbs,
                                   List<List<Reg>> registos) {
        linha("");
        linha("--- 6. Mensagens e bytes por registo ---");
        linha("LOTE: bytes dos lotes com registos (cabecalho incluido) / registos transportados.");
        linha("REG (COMINDIR03): REG;127.0.0.1;<porto>;<linha do registo>.");

        long totalLotes = 0;
        long totalRegistos = 0;
        long totalBytesLote = 0;
        long totalRegistosEmLote = 0;
        long totalBytesReg = 0;

        for (int k = 0; k < n; k++) {
            long lotes = hbs.get(k).size();
            long bytesLote = 0;
            long registosEmLote = 0;

            for (Hb h : hbs.get(k)) {
                if (h.n > 0) {
                    bytesLote += h.bytes;
                    registosEmLote += h.n;
                }
            }

            long bytesReg = 0;

            for (Reg r : registos.get(k)) {
                String reg = "REG;127.0.0.1;" + (Wire.PORTO_BASE + k)
                        + ";" + r.linha;
                bytesReg += reg.getBytes(StandardCharsets.UTF_8).length;
            }

            long numRegistos = registos.get(k).size();

            linha("Particao %d: mensagens COMINDIR03=%d (REG) / COMINDIR04=%d (LOTE) | "
                            + "bytes por registo: REG=%s LOTE=%s",
                    k, numRegistos, lotes,
                    numRegistos == 0 ? "n/d"
                            : String.format(Locale.US, "%.1f", (double) bytesReg / numRegistos),
                    registosEmLote == 0 ? "n/d"
                            : String.format(Locale.US, "%.1f", (double) bytesLote / registosEmLote));

            totalLotes += lotes;
            totalRegistos += numRegistos;
            totalBytesLote += bytesLote;
            totalRegistosEmLote += registosEmLote;
            totalBytesReg += bytesReg;
        }

        linha("Total: mensagens COMINDIR03=%d / COMINDIR04=%d | bytes por registo: "
                        + "REG=%s LOTE=%s",
                totalRegistos, totalLotes,
                totalRegistos == 0 ? "n/d"
                        : String.format(Locale.US, "%.1f", (double) totalBytesReg / totalRegistos),
                totalRegistosEmLote == 0 ? "n/d"
                        : String.format(Locale.US, "%.1f",
                        (double) totalBytesLote / totalRegistosEmLote));
    }
}
