package raft04;

public final class VoteReply {
    public final long term;
    public final int voter;
    public final int candidate;
    public final boolean granted;

    public VoteReply(long term, int voter, int candidate, boolean granted) {
        this.term = term;
        this.voter = voter;
        this.candidate = candidate;
        this.granted = granted;
    }

    public String encode() {
        return "VOTO;" + term + ";" + voter + ";" + candidate + ";"
                + (granted ? "S" : "N");
    }

    public static VoteReply decode(String message) {
        String[] f = message.split(";");

        if (f.length != 5 || !"VOTO".equals(f[0])) {
            throw new IllegalArgumentException("VOTO invalido: " + message);
        }

        if (!"S".equals(f[4]) && !"N".equals(f[4])) {
            throw new IllegalArgumentException("Resposta de voto invalida: " + message);
        }

        return new VoteReply(
                Long.parseLong(f[1]),
                Integer.parseInt(f[2]),
                Integer.parseInt(f[3]),
                "S".equals(f[4])
        );
    }
}