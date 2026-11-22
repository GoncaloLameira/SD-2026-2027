
package tpf;

public final class Wire {

    // Tamanho máximo de um chunk: 64 KiB
    public static final int CHUNK_MAX_BYTES =
            Integer.getInteger("tpf.chunk.max.bytes", 65536);

    // Padding em múltiplos de 4096 bytes
    public static final int PAD_BYTES =
            Integer.getInteger("tpf.pad.bytes", 4096);

    // Tempo máximo para selar um chunk: 5 segundos
    public static final long SELAGEM_MS =
            Long.getLong("tpf.selagem.ms", 5000L);

    private Wire() {
        // Impede a criação de instâncias
    }

    static {
        if (CHUNK_MAX_BYTES <= 0 ||
                PAD_BYTES <= 0 ||
                SELAGEM_MS <= 0) {
            throw new IllegalArgumentException(
                    "Os parâmetros do arquivo devem ser positivos."
            );
        }
    }
}

