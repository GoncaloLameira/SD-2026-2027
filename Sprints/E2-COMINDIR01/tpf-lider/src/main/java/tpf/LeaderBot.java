package tpf;

import io.github.cdimascio.dotenv.Dotenv;
import net.dv8tion.jda.api.JDA;
import net.dv8tion.jda.api.JDABuilder;
import net.dv8tion.jda.api.entities.Guild;
import net.dv8tion.jda.api.interactions.commands.OptionType;
import net.dv8tion.jda.api.interactions.commands.build.Commands;
import net.dv8tion.jda.api.requests.GatewayIntent;

public class LeaderBot {

    public static void main(String[] args) throws InterruptedException {

        Dotenv dotenv = Dotenv.load();
        String token = dotenv.get("DISCORD_TOKEN");

        if (token == null || token.isBlank()) {
            System.err.println("Erro: DISCORD_TOKEN não definido no .env.");
            return;
        }

        JDA jda = JDABuilder.createDefault(token)
                .enableIntents(GatewayIntent.MESSAGE_CONTENT)
                .addEventListeners(new BotListener())
                .build();

        jda.awaitReady();

        Guild guild = jda.getGuilds().stream()
                .filter(g -> g.getName().equals("SDT - TPF"))
                .findFirst()
                .orElse(null);

        if (guild == null) {
            System.err.println("Erro: servidor 'SDT - TPF' não encontrado.");
            jda.shutdown();
            return;
        }

        guild.updateCommands().addCommands(
                Commands.slash("registar", "Regista um texto no arquivo")
                        .addOption(OptionType.STRING, "texto", "Texto a registar", true),

                Commands.slash("estado", "Mostra o estado do arquivo"),

                Commands.slash("config", "Altera a configuração do arquivo")
                        .addOption(OptionType.INTEGER, "r", "Número de réplicas", true),

                Commands.slash("recuperar", "Recupera registos num intervalo")
                        .addOption(OptionType.INTEGER, "de", "Início do intervalo", true)
                        .addOption(OptionType.INTEGER, "ate", "Fim do intervalo", true),

                Commands.slash("replica-token", "Gera um token para uma réplica"),

                Commands.slash("replica-remover", "Remove uma réplica")
                        .addOption(OptionType.STRING, "id", "Identificador da réplica", true),

                Commands.slash("rajada", "Gera uma rajada de registos")
                        .addOption(OptionType.INTEGER, "n", "Número de registos", true)
                        .addOption(
                                OptionType.INTEGER,
                                "intervaloms",
                                "Intervalo entre registos em ms",
                                true
                        )
        ).queue();

        System.out.println(
                "Comandos registados no servidor: " + guild.getName()
        );

        System.out.println("LeaderBot ligado ao Discord.");
    }
}