package com.jqueue.application.processor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.jupiter.api.Test;

import com.jqueue.application.services.JobHandler;
import com.jqueue.common.errors.AppError;
import com.jqueue.domain.job.vo.TaskType;

class DefaultHandlerRegistryTest {

    static final JobHandler NOOP = job -> {
    };

    @Test
    void resolvesARegisteredHandler() {
        DefaultHandlerRegistry registry = new DefaultHandlerRegistry();
        registry.register(new TaskType("email.send"), NOOP);

        assertThat(registry.resolve(new TaskType("email.send"))).isSameAs(NOOP);
    }

    @Test
    void rejectsADuplicateRegistration() {
        DefaultHandlerRegistry registry = new DefaultHandlerRegistry();
        registry.register(new TaskType("email.send"), NOOP);

        // Overwriting silently would mean jobs stop running the code someone thinks they
        // run — far harder to diagnose than a startup failure.
        assertThatThrownBy(() -> registry.register(new TaskType("email.send"), NOOP))
                .isInstanceOf(AppError.class)
                .hasMessageContaining("duplicate_handler");
    }

    @Test
    void rejectsAnEmptyTaskTypeOrNullHandler() {
        DefaultHandlerRegistry registry = new DefaultHandlerRegistry();

        assertThatThrownBy(() -> registry.register(new TaskType(""), NOOP)).isInstanceOf(AppError.class);
        assertThatThrownBy(() -> registry.register(new TaskType("email.send"), null)).isInstanceOf(AppError.class);
    }

    @Test
    void reportsAnUnknownTaskType() {
        DefaultHandlerRegistry registry = new DefaultHandlerRegistry();

        assertThatThrownBy(() -> registry.resolve(new TaskType("never.registered")))
                .isInstanceOf(AppError.class)
                .hasMessageContaining("handler_not_found");
    }

    @Test
    void listsRegisteredTypesInSortedOrder() {
        DefaultHandlerRegistry registry = new DefaultHandlerRegistry();
        registry.register(new TaskType("zeta.task"), NOOP);
        registry.register(new TaskType("alpha.task"), NOOP);

        assertThat(registry.registeredTypes())
                .containsExactly(new TaskType("alpha.task"), new TaskType("zeta.task"));
    }

    @Test
    void isSafeForConcurrentUse() throws Exception {
        DefaultHandlerRegistry registry = new DefaultHandlerRegistry();
        registry.register(new TaskType("email.send"), NOOP);

        // Every worker thread resolves against this map on every job, while a late
        // registration may still be happening; none of it may throw or lose an entry.
        try (ExecutorService pool = Executors.newFixedThreadPool(16)) {
            List<Future<?>> futures = new ArrayList<>();
            for (int i = 0; i < 16; i++) {
                int n = i;
                futures.add(pool.submit(() -> {
                    registry.register(new TaskType("late.task." + n), NOOP);
                    for (int j = 0; j < 1000; j++) {
                        registry.resolve(new TaskType("email.send"));
                        registry.registeredTypes();
                    }
                }));
            }
            for (Future<?> f : futures) {
                f.get();
            }
        }
        assertThat(registry.registeredTypes()).hasSize(17);
    }
}
