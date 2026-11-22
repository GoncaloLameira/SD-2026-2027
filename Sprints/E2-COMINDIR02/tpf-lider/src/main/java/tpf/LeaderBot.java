
package tpf;

import io.github.cdimascio.dotenv.Dotenv;
import net.dv8tion.jda.api.JDA;
import net.dv8tion.jda.api.JDABuilder;
import net.dv8tion.jda.api.entities.Guild;
import net.dv8tion.jda.api.interactions.commands.OptionType;
import net.dv8tion.jda.api.interactions.commands.build.Commands;
import net.dv8tion.jda.api.requests.GatewayIntent;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

public class LeaderBot {

    public static void main(String[] args) throws InterruptedException {

        // 1. Carrega ou cria a chave mestra de 32 bytes.
        try {
            byte[] masterKey = MasterKey.loadOrCreate();

            System.out.println(
                    "Chave mestra carregada com sucesso ("
                            + masterKey.length + " bytes)."
            );

            // Limpa a copia temporaria da chave.
            Arrays.fill(masterKey, (byte) 0);

        } catch (IOException e) {
            System.err.println(
                    "Erro ao carregar a chave mestra: "
                            + e.getMessage()
            );
            return;
        }

        // 2. Carrega o token do Discord a partir do .env.
        Dotenv dotenv = Dotenv.load();
        String token = dotenv.get("DISCORD_TOKEN");

        if (token == null || token.isBlank()) {
            System.err.println(
                    "Erro: DISCORD_TOKEN nao definido no .env."
            );
            return;
        }

        // 3. Inicializa o armazenamento cifrado e o gestor de chunks.
        ChunkStorage chunkStorage = null;
        ChunkManager chunkManager = null;
        RegistoLog registoLog;

        try {
            chunkStorage = new ChunkStorage();

            chunkManager = new ChunkManager(chunkStorage);

            registoLog = new RegistoLog(chunkManager);

            System.out.println(
                    "Sistema de chunks cifrados iniciado."
            );

            // 4. Identifica os registos ja arquivados.
            Set<Long> arquivados =
                    ChunkRecovery.identificarArquivados(registoLog);

            // Le todos os registos persistidos no registos.log.
            List<Registo> registos = registoLog.lerTodos();

            int recuperados = 0;

            // Reencaminha apenas os registos ainda nao cifrados.
            for (Registo registo : registos) {

                if (!arquivados.contains(registo.lsn())) {

                    chunkManager.adicionar(registo);
                    recuperados++;

                    System.out.println(
                            "REGISTO REENCAMINHADO | LSN="
                                    + registo.lsn()
                    );
                }
            }

            System.out.println(
                    "RECUPERACAO CONCLUIDA | pendentes encaminhados="
                            + recuperados
            );

        } catch (IOException | RuntimeException e) {
            System.err.println(
                    "Erro ao iniciar ou recuperar o arquivo: "
                            + e.getMessage()
            );

            // Tenta fechar o gestor sem perder chunks pendentes.
            if (chunkManager != null) {
                try {
                    chunkManager.close();

                } catch (IOException erro) {
                    System.err.println(
                            "Erro ao fechar ChunkManager: "
                                    + erro.getMessage()
                    );
                }
            }

            if (chunkStorage != null) {
                chunkStorage.close();
            }

            return;
        }

        // 5. Guarda referencias finais para o shutdown hook.
        final ChunkStorage storageFinal = chunkStorage;
        final ChunkManager managerFinal = chunkManager;

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {

            try {
                managerFinal.close();

                System.out.println(
                        "ChunkManager encerrado."
                );

            } catch (IOException e) {
                System.err.println(
                        "Erro ao encerrar ChunkManager: "
                                + e.getMessage()
                );

            } finally {
                storageFinal.close();
            }

        }, "tpf-shutdown"));

        // 6. Cria a ligacao ao Discord.
        JDA jda = JDABuilder.createDefault(token)
                .enableIntents(GatewayIntent.MESSAGE_CONTENT)
                .addEventListeners(new BotListener(registoLog))
                .build();

        // Aguarda ate o bot estar ligado.
        jda.awaitReady();

        // 7. Encontra o servidor do Discord.
        Guild guild = jda.getGuilds().stream()
                .filter(g -> g.getName().equals("SDT - TPF"))
                .findFirst()
                .orElse(null);

        if (guild == null) {
            System.err.println(
                    "Erro: servidor 'SDT - TPF' nao encontrado."
            );

            jda.shutdown();
            return;
        }

        // 8. Regista os comandos slash no servidor Discord.
        guild.updateCommands().addCommands(

                // /registar <texto>
                Commands.slash(
                        "registar",
                        "Regista um texto no arquivo"
                ).addOption(
                        OptionType.STRING,
                        "texto",
                        "Texto a registar",
                        true
                ),

                // /estado
                Commands.slash(
                        "estado",
                        "Mostra o estado do arquivo"
                ),

                // /config r <n>
                Commands.slash(
                        "config",
                        "Altera a configuracao do arquivo"
                ).addOption(
                        OptionType.INTEGER,
                        "r",
                        "Numero de replicas",
                        true
                ),

                // /recuperar <de> <ate>
                Commands.slash(
                        "recuperar",
                        "Recupera registos num intervalo"
                ).addOption(
                        OptionType.INTEGER,
                        "de",
                        "Inicio do intervalo",
                        true
                ).addOption(
                        OptionType.INTEGER,
                        "ate",
                        "Fim do intervalo",
                        true
                ),

                // /replica-token
                Commands.slash(
                        "replica-token",
                        "Gera um token para uma replica"
                ),

                // /replica-remover <id>
                Commands.slash(
                        "replica-remover",
                        "Remove uma replica"
                ).addOption(
                        OptionType.STRING,
                        "id",
                        "Identificador da replica",
                        true
                ),

                // /rajada <n> <intervaloMs>
                Commands.slash(
                        "rajada",
                        "Gera uma rajada de registos"
                ).addOption(
                        OptionType.INTEGER,
                        "n",
                        "Numero de registos",
                        true
                ).addOption(
                        OptionType.INTEGER,
                        "intervaloms",
                        "Intervalo entre registos em ms",
                        true
                )

        ).queue();

        // 9. Mensagens de confirmacao.
        System.out.println(
                "Comandos registados no servidor: "
                        + guild.getName()
        );

        System.out.println(
                "LeaderBot ligado ao Discord."
        );
    }
}
