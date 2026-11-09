
package raft04;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;

public final class NodeMessaging {

    @FunctionalInterface
    public interface MessageHandler {
        void handle(String message) throws IOException;
    }

    private final int partition;
    private final IpfsPubSub pubSub;
    private final BooleanSupplier isolated;
    private final Consumer<String> logger;

    public NodeMessaging(
            int partition,
            IpfsPubSub pubSub,
            BooleanSupplier isolated,
            Consumer<String> logger) {

        this.partition = partition;
        this.pubSub = pubSub;
        this.isolated = isolated;
        this.logger = logger;
    }

    public void startSubscriptions(
            Consumer<String> inputHandler,
            MessageHandler batchHandler,
            MessageHandler electionHandler) {

        subscribe(Wire.topicIn(partition), inputHandler::accept);
        subscribe(Wire.topicData(partition), batchHandler);
        subscribe(Wire.topicElection(partition), electionHandler);
    }

    private void subscribe(String topic, MessageHandler handler) {

        Thread thread = new Thread(() -> {

            try {
                pubSub.subscribe(topic, message -> {

                    if (isolated.getAsBoolean()) {
                        return;
                    }

                    try {
                        handler.handle(message);

                    } catch (IOException | RuntimeException e) {
                        logger.accept(
                                "ERRO_RECECAO;topico=" + topic
                                        + ";detalhe=" + e.getMessage()
                        );
                    }
                });

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();

            } catch (IOException e) {
                logger.accept(
                        "ERRO_SUBSCRICAO;topico=" + topic
                                + ";detalhe=" + e.getMessage()
                );
            }

        }, "sub-" + topic + "-p" + partition);

        thread.setDaemon(true);
        thread.start();
    }

    public void publish(String topic, String message)
            throws IOException {

        if (isolated.getAsBoolean()) {
            logger.accept(
                    "PUBLICACAO_IGNORADA;topico=" + topic
                            + ";motivo=ISOLADO"
            );
            return;
        }

        try {
            pubSub.publish(topic, message);

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Publicação interrompida.", e);
        }
    }
}
