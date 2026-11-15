package io.github.inbox.loyalty.events;

import io.github.dmitriyiliyov.oncebox.core.inbox.EventIdExtractor;
import io.github.dmitriyiliyov.oncebox.core.inbox.IdempotentConsumer;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;

/**
 * An inbox kept in memory: records the id of every event it lets through and skips an event whose id it already
 * recorded, as the real inbox does without a database.
 */
class InMemoryIdempotentConsumer implements IdempotentConsumer {

    private final Set<UUID> consumedIds = new LinkedHashSet<>();

    List<UUID> consumedIds() {
        return new ArrayList<>(consumedIds);
    }

    @Override
    public void consume(UUID eventId, Runnable operation) {
        if (consumedIds.add(eventId)) {
            operation.run();
        }
    }

    @Override
    public <T> void consume(T message, EventIdExtractor<T> idExtractor, Consumer<T> operation) {
        if (consumedIds.add(idExtractor.extract(message))) {
            operation.accept(message);
        }
    }

    @Override
    public void consume(Set<UUID> ids, Consumer<Set<UUID>> operation) {
        Set<UUID> fresh = new LinkedHashSet<>();
        ids.forEach(id -> {
            if (consumedIds.add(id)) {
                fresh.add(id);
            }
        });
        operation.accept(fresh);
    }

    @Override
    public <T> void consume(List<T> messages, EventIdExtractor<T> idExtractor, Consumer<List<T>> operation) {
        List<T> fresh = messages.stream()
                .filter(message -> consumedIds.add(idExtractor.extract(message)))
                .toList();
        operation.accept(fresh);
    }
}
