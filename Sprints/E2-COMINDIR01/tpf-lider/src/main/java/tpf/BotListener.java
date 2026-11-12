package tpf;

import net.dv8tion.jda.api.events.interaction.command.SlashCommandInteractionEvent;
import net.dv8tion.jda.api.hooks.ListenerAdapter;
import net.dv8tion.jda.api.events.message.MessageReceivedEvent;

public class BotListener extends ListenerAdapter {

    @Override
    public void onSlashCommandInteraction(SlashCommandInteractionEvent event) {

        switch (event.getName()) {

            // /registar <texto>
            case "registar" -> {
                String texto = event.getOption("texto").getAsString();

                if (texto.isBlank()) {
                    event.reply("O texto não pode estar vazio.")
                            .setEphemeral(true)
                            .queue();
                    return;
                }

                event.reply(
                        "/registar recebido: texto=" + texto +
                                " — ainda não implementado"
                ).queue();
            }

            // /estado
            case "estado" ->
                    event.reply(
                            "/estado recebido — ainda não implementado"
                    ).queue();

            // /config r <n>
            // Apenas admin-arquivo
            case "config" -> {
                if (!isAdmin(event)) {
                    event.reply(
                                    "Sem permissão: é necessário o cargo admin-arquivo."
                            )
                            .setEphemeral(true)
                            .queue();
                    return;
                }

                long r = event.getOption("r").getAsLong();

                if (r <= 0) {
                    event.reply(
                                    "Argumento inválido: r deve ser superior a 0."
                            )
                            .setEphemeral(true)
                            .queue();
                    return;
                }

                event.reply(
                        "/config recebido: r=" + r +
                                " — ainda não implementado"
                ).queue();
            }

            // /recuperar <de> <ate>
            case "recuperar" -> {
                long de = event.getOption("de").getAsLong();
                long ate = event.getOption("ate").getAsLong();

                if (de < 0 || ate < de) {
                    event.reply(
                                    "Argumentos inválidos: deve verificar-se 0 <= de <= ate."
                            )
                            .setEphemeral(true)
                            .queue();
                    return;
                }

                event.reply(
                        "/recuperar recebido: de=" + de +
                                ", ate=" + ate +
                                " — ainda não implementado"
                ).queue();
            }

            // /replica-token
            // Apenas admin-arquivo
            case "replica-token" -> {
                if (!isAdmin(event)) {
                    event.reply(
                                    "Sem permissão: é necessário o cargo admin-arquivo."
                            )
                            .setEphemeral(true)
                            .queue();
                    return;
                }

                event.reply(
                        "/replica-token recebido — ainda não implementado"
                ).queue();
            }

            // /replica-remover <id>
            // Apenas admin-arquivo
            case "replica-remover" -> {
                if (!isAdmin(event)) {
                    event.reply(
                                    "Sem permissão: é necessário o cargo admin-arquivo."
                            )
                            .setEphemeral(true)
                            .queue();
                    return;
                }

                String id = event.getOption("id").getAsString();

                if (id.isBlank()) {
                    event.reply(
                                    "Argumento inválido: o id não pode estar vazio."
                            )
                            .setEphemeral(true)
                            .queue();
                    return;
                }

                event.reply(
                        "/replica-remover recebido: id=" + id +
                                " — ainda não implementado"
                ).queue();
            }

            // /rajada <n> <intervaloMs>
            // Apenas admin-arquivo
            case "rajada" -> {
                if (!isAdmin(event)) {
                    event.reply(
                                    "Sem permissão: é necessário o cargo admin-arquivo."
                            )
                            .setEphemeral(true)
                            .queue();
                    return;
                }

                long n = event.getOption("n").getAsLong();
                long intervaloMs = event.getOption("intervaloms").getAsLong();

                if (n <= 0 || intervaloMs < 0) {
                    event.reply(
                                    "Argumentos inválidos: n deve ser > 0 e intervaloMs >= 0."
                            )
                            .setEphemeral(true)
                            .queue();
                    return;
                }

                event.reply(
                        "/rajada recebido: n=" + n +
                                ", intervaloMs=" + intervaloMs +
                                " — ainda não implementado"
                ).queue();
            }

            default ->
                    event.reply("Comando desconhecido.")
                            .setEphemeral(true)
                            .queue();
        }
    }

    /**
     * Verifica se o utilizador que executou o comando
     * possui o cargo "admin-arquivo".
     */
    private boolean isAdmin(SlashCommandInteractionEvent event) {

        // Se o comando não tiver sido executado por um membro
        // de um servidor, não pode ser considerado administrador.
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
    public void onMessageReceived(MessageReceivedEvent event) {

        // Ignora mensagens enviadas por bots
        if (event.getAuthor().isBot()) {
            return;
        }

        // Ignora mensagens que não tenham sido enviadas num servidor
        if (!event.isFromGuild()) {
            return;
        }

        // Ignora mensagens enviadas fora do canal #arquivo
        if (!event.getChannel().getName().equals("arquivo")) {
            return;
        }

        String texto = event.getMessage().getContentRaw();

        System.out.println(
                "Mensagem recebida em #arquivo: " + texto
        );
    }
}