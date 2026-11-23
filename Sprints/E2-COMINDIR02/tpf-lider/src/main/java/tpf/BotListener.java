
package tpf;

import net.dv8tion.jda.api.events.interaction.command.SlashCommandInteractionEvent;
import net.dv8tion.jda.api.events.message.MessageReceivedEvent;
import net.dv8tion.jda.api.hooks.ListenerAdapter;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

public class BotListener extends ListenerAdapter {

    // Responsavel por guardar os registos em disco.
    private final RegistoLog registoLog;

    // Executa rajadas sem bloquear os eventos do Discord.
    private final ExecutorService executorRajada =
            Executors.newSingleThreadExecutor(r -> {
                Thread thread = new Thread(r, "tpf-rajada");
                thread.setDaemon(true);
                return thread;
            });

    // Impede a execucao simultanea de varias rajadas.
    private final AtomicBoolean rajadaEmCurso =
            new AtomicBoolean(false);

    private static final int MAX_RAJADA = 5000;
    private static final long MAX_INTERVALO_MS = 60000;

    public BotListener(RegistoLog registoLog) {

        if (registoLog == null) {
            throw new IllegalArgumentException(
                    "RegistoLog nao pode ser nulo."
            );
        }

        this.registoLog = registoLog;
    }

    @Override
    public void onSlashCommandInteraction(
            SlashCommandInteractionEvent event
    ) {

        switch (event.getName()) {

            // /registar <texto>
            case "registar" -> {

                String texto =
                        event.getOption("texto").getAsString();

                if (texto.isBlank()) {
                    event.reply("O texto nao pode estar vazio.")
                            .setEphemeral(true)
                            .queue();
                    return;
                }

                event.deferReply(true).queue(hook -> {

                    try {
                        Registo registo = registoLog.registar(
                                event.getUser().getId(),
                                event.getChannel().getId(),
                                texto
                        );

                        hook.editOriginal(
                                "Registo guardado com sucesso! LSN="
                                        + registo.lsn()
                        ).queue();

                        System.out.println(
                                "REGISTO | LSN=" + registo.lsn()
                                        + " | autor=" + registo.autor()
                        );

                    } catch (IOException e) {

                        System.err.println(
                                "Erro ao guardar registo: "
                                        + e.getMessage()
                        );

                        hook.editOriginal(
                                "Erro ao guardar o registo."
                        ).queue();
                    }
                });
            }

            // /estado
            case "estado" -> {

                event.reply(
                        "/estado recebido - ainda nao implementado"
                ).queue();
            }

            // /config r <n>
            case "config" -> {

                if (!isAdmin(event)) {

                    event.reply(
                            "Sem permissao: e necessario o cargo admin-arquivo."
                    ).setEphemeral(true).queue();

                    return;
                }

                long r = event.getOption("r").getAsLong();

                if (r <= 0) {

                    event.reply(
                            "Argumento invalido: r deve ser superior a 0."
                    ).setEphemeral(true).queue();

                    return;
                }

                event.reply(
                        "/config recebido: r=" + r
                                + " - ainda nao implementado"
                ).queue();
            }

            // /recuperar <de> <ate>
            case "recuperar" -> {

                long de = event.getOption("de").getAsLong();
                long ate = event.getOption("ate").getAsLong();

                if (de < 0 || ate < de) {

                    event.reply(
                            "Argumentos invalidos: 0 <= de <= ate."
                    ).setEphemeral(true).queue();

                    return;
                }

                event.reply(
                        "/recuperar recebido: de=" + de
                                + ", ate=" + ate
                                + " - ainda nao implementado"
                ).queue();
            }

            // /replica-token
            case "replica-token" -> {

                if (!isAdmin(event)) {

                    event.reply(
                            "Sem permissao: e necessario o cargo admin-arquivo."
                    ).setEphemeral(true).queue();

                    return;
                }

                event.reply(
                        "/replica-token recebido - ainda nao implementado"
                ).queue();
            }

            // /replica-remover <id>
            case "replica-remover" -> {

                if (!isAdmin(event)) {

                    event.reply(
                            "Sem permissao: e necessario o cargo admin-arquivo."
                    ).setEphemeral(true).queue();

                    return;
                }

                String id =
                        event.getOption("id").getAsString();

                if (id.isBlank()) {

                    event.reply(
                            "Argumento invalido: id nao pode estar vazio."
                    ).setEphemeral(true).queue();

                    return;
                }

                event.reply(
                        "/replica-remover recebido: id=" + id
                                + " - ainda nao implementado"
                ).queue();
            }

            // /rajada <n> <intervaloms>
            case "rajada" -> {

                // Apenas utilizadores com cargo admin-arquivo.
                if (!isAdmin(event)) {

                    event.reply(
                            "Sem permissao: e necessario o cargo admin-arquivo."
                    ).setEphemeral(true).queue();

                    return;
                }

                long n = event.getOption("n").getAsLong();

                long intervaloMs = event.getOption(
                        "intervaloms"
                ).getAsLong();

                // Validacao dos argumentos.
                if (n <= 0 || n > MAX_RAJADA
                        || intervaloMs < 0
                        || intervaloMs > MAX_INTERVALO_MS) {

                    event.reply(
                            "Argumentos invalidos. "
                                    + "n deve estar entre 1 e "
                                    + MAX_RAJADA
                                    + " e intervaloms entre 0 e "
                                    + MAX_INTERVALO_MS + "."
                    ).setEphemeral(true).queue();

                    return;
                }

                // Nao permite duas rajadas em simultaneo.
                if (!rajadaEmCurso.compareAndSet(
                        false, true
                )) {

                    event.reply(
                            "Ja existe uma rajada em execucao."
                    ).setEphemeral(true).queue();

                    return;
                }

                // Guarda os dados antes de executar a tarefa.
                final String autor =
                        event.getUser().getId();

                final String canal =
                        event.getChannel().getId();

                final int quantidade = (int) n;
                final long intervalo = intervaloMs;

                // Identificador da rajada para os testes.
                final String identificador =
                        String.valueOf(System.currentTimeMillis());

                // Resposta privada, atualizada no fim.
                event.deferReply(true).queue(hook -> {

                    try {
                        executorRajada.execute(() -> {

                            int criados = 0;
                            long primeiroLsn = -1;
                            long ultimoLsn = -1;

                            long inicio =
                                    System.currentTimeMillis();

                            System.out.println(
                                    "RAJADA INICIADA | id="
                                            + identificador
                                            + " | n=" + quantidade
                                            + " | intervaloMs="
                                            + intervalo
                            );

                            try {

                                for (int i = 1;
                                     i <= quantidade;
                                     i++) {

                                    if (Thread.currentThread()
                                            .isInterrupted()) {
                                        throw new InterruptedException(
                                                "Rajada interrompida."
                                        );
                                    }

                                    // Mensagem de teste com tamanho
                                    // suficiente para exercitar
                                    // o limite de 64 KiB.
                                    String texto =
                                            "RAJADA | id="
                                                    + identificador
                                                    + " | registo="
                                                    + i + "/"
                                                    + quantidade
                                                    + " | dados="
                                                    + "X".repeat(220);

                                    // Usa a mesma persistencia
                                    // dos comandos e mensagens normais.
                                    Registo registo =
                                            registoLog.registar(
                                                    "rajada:" + autor,
                                                    canal,
                                                    texto
                                            );

                                    if (primeiroLsn == -1) {
                                        primeiroLsn = registo.lsn();
                                    }

                                    ultimoLsn = registo.lsn();
                                    criados++;

                                    // Mostra progresso a cada 50
                                    // registos e no ultimo.
                                    if (criados % 50 == 0
                                            || criados == quantidade) {

                                        System.out.println(
                                                "RAJADA PROGRESSO | "
                                                        + criados + "/"
                                                        + quantidade
                                                        + " | LSN="
                                                        + ultimoLsn
                                        );
                                    }

                                    // Intervalo entre os registos.
                                    if (intervalo > 0
                                            && i < quantidade) {

                                        Thread.sleep(intervalo);
                                    }
                                }

                                long duracao =
                                        System.currentTimeMillis()
                                                - inicio;

                                System.out.println(
                                        "RAJADA CONCLUIDA | id="
                                                + identificador
                                                + " | criados="
                                                + criados
                                                + " | LSN="
                                                + primeiroLsn
                                                + ".." + ultimoLsn
                                                + " | duracaoMs="
                                                + duracao
                                );

                                hook.editOriginal(
                                        "Rajada concluida!\n"
                                                + "Registos criados: "
                                                + criados + "\n"
                                                + "LSN: "
                                                + primeiroLsn
                                                + " ate "
                                                + ultimoLsn + "\n"
                                                + "Duracao: "
                                                + duracao + " ms"
                                ).queue();

                            } catch (IOException e) {

                                System.err.println(
                                        "RAJADA ERRO | "
                                                + e.getMessage()
                                );

                                hook.editOriginal(
                                        "Erro durante a rajada.\n"
                                                + "Registos criados: "
                                                + criados + "/"
                                                + quantidade + "\n"
                                                + "Ultimo LSN: "
                                                + ultimoLsn
                                ).queue();

                            } catch (InterruptedException e) {

                                Thread.currentThread().interrupt();

                                System.err.println(
                                        "RAJADA INTERROMPIDA"
                                );

                                hook.editOriginal(
                                        "Rajada interrompida.\n"
                                                + "Registos criados: "
                                                + criados + "/"
                                                + quantidade
                                ).queue();

                            } finally {

                                rajadaEmCurso.set(false);
                            }

                        });

                    } catch (RejectedExecutionException e) {

                        rajadaEmCurso.set(false);

                        hook.editOriginal(
                                "Nao foi possivel iniciar a rajada."
                        ).queue();
                    }

                }, erro -> {
                    rajadaEmCurso.set(false);

                    System.err.println(
                            "Erro ao iniciar resposta da rajada: "
                                    + erro.getMessage()
                    );
                });
            }

            default -> event.reply(
                    "Comando desconhecido."
            ).setEphemeral(true).queue();
        }
    }

    // Verifica o cargo admin-arquivo.
    private boolean isAdmin(
            SlashCommandInteractionEvent event
    ) {

        if (event.getMember() == null) {
            return false;
        }

        return event.getMember()
                .getRoles()
                .stream()
                .anyMatch(role ->
                        role.getName().equals("admin-arquivo")
                );
    }

    @Override
    public void onMessageReceived(
            MessageReceivedEvent event
    ) {

        // Ignora mensagens enviadas por bots.
        if (event.getAuthor().isBot()) {
            return;
        }

        // Ignora mensagens privadas.
        if (!event.isFromGuild()) {
            return;
        }

        // So processa mensagens do canal #arquivo.
        if (!event.getChannel()
                .getName().equals("arquivo")) {
            return;
        }

        String texto =
                event.getMessage().getContentRaw();

        if (texto.isBlank()) {
            return;
        }

        try {

            Registo registo = registoLog.registar(
                    event.getAuthor().getId(),
                    event.getChannel().getId(),
                    texto
            );

            System.out.println(
                    "Mensagem #arquivo guardada | LSN="
                            + registo.lsn()
            );

        } catch (IOException e) {

            System.err.println(
                    "Erro ao guardar mensagem #arquivo: "
                            + e.getMessage()
            );
        }
    }
}
