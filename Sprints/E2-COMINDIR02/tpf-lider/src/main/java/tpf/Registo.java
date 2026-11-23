
package tpf;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

public record Registo(
        long lsn,
        long timestamp,
        String autor,
        String canal,
        String texto
) {

    public Registo {
        if (lsn <= 0) {
            throw new IllegalArgumentException(
                    "O LSN deve ser superior a zero."
            );
        }

        if (autor == null || canal == null || texto == null) {
            throw new IllegalArgumentException(
                    "Os campos do registo nao podem ser nulos."
            );
        }
    }

    // Converte o registo para uma linha de texto.
    // Base64 evita problemas com quebras de linha
    // e caracteres separadores no conteudo.
    public String toLine() {
        return lsn + ";" +
                timestamp + ";" +
                encode(autor) + ";" +
                encode(canal) + ";" +
                encode(texto);
    }

    // Reconstrói um registo a partir de uma linha.
    public static Registo fromLine(String linha) {
        String[] campos = linha.split(";", -1);

        if (campos.length != 5) {
            throw new IllegalArgumentException(
                    "Formato de registo invalido."
            );
        }

        return new Registo(
                Long.parseLong(campos[0]),
                Long.parseLong(campos[1]),
                decode(campos[2]),
                decode(campos[3]),
                decode(campos[4])
        );
    }

    private static String encode(String valor) {
        return Base64.getEncoder().encodeToString(
                valor.getBytes(StandardCharsets.UTF_8)
        );
    }

    private static String decode(String valor) {
        return new String(
                Base64.getDecoder().decode(valor),
                StandardCharsets.UTF_8
        );
    }
}

