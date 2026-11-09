package raft04;

public final class VoteRequest {
    public final long term;
    public final int candidate;
    public final long lastSeq;
    public final long lastTerm;

    public VoteRequest(long term, int candidate, long lastSeq, long lastTerm) {
        this.term = term;
        this.candidate = candidate;
        this.lastSeq = lastSeq;
        this.lastTerm = lastTerm;
    }

    public String encode() {
        return "PEDIDO_VOTO;" + term + ";" + candidate + ";" + lastSeq + ";" + lastTerm;
    }

    public static VoteRequest decode(String message) {
        String[] f = message.split(";");
        if (f.length != 5 || !"PEDIDO_VOTO".equals(f[0])) {
            throw new IllegalArgumentException("PEDIDO_VOTO invalido: " + message);
        }

        return new VoteRequest(
                Long.parseLong(f[1]),
                Integer.parseInt(f[2]),
                Long.parseLong(f[3]),
                Long.parseLong(f[4])
        );
    }
}