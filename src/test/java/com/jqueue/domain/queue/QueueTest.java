package com.jqueue.domain.queue;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;

import org.junit.jupiter.api.Test;

import com.jqueue.common.errors.AppError;
import com.jqueue.domain.job.vo.QueueName;

class QueueTest {

    static Queue queue(String name, int weight) {
        return Queue.create(QueueName.of(name), Weight.of(weight));
    }

    static List<String> names(List<QueueName> order) {
        return order.stream().map(QueueName::value).toList();
    }

    @Test
    void buildDequeueOrder_repeatsEachQueueByItsWeight() {
        List<QueueName> order = Queue.buildDequeueOrder(List.of(
                queue("low", 1), queue("critical", 3), queue("default", 2)));

        // Heaviest first, each repeated by its weight: this is what stops a low-traffic
        // queue from starving while still letting critical lead.
        assertThat(names(order)).containsExactly(
                "critical", "critical", "critical",
                "default", "default",
                "low");
    }

    @Test
    void buildDequeueOrder_excludesPausedQueues() {
        Queue paused = queue("critical", 3);
        paused.pause();

        List<QueueName> order = Queue.buildDequeueOrder(List.of(paused, queue("default", 1)));

        assertThat(names(order)).containsExactly("default");
    }

    @Test
    void buildDequeueOrder_isDeterministicForEqualWeights() {
        // Two queues of equal weight must produce the same order on every call, or two
        // workers would disagree about which queue leads and the fairness guarantee would
        // only hold on average.
        List<QueueName> first = Queue.buildDequeueOrder(List.of(queue("beta", 2), queue("alpha", 2)));
        List<QueueName> second = Queue.buildDequeueOrder(List.of(queue("alpha", 2), queue("beta", 2)));

        assertThat(first).isEqualTo(second);
        assertThat(first.getFirst().value()).isEqualTo("alpha");
    }

    @Test
    void pauseAndResume() {
        Queue queue = queue("default", 1);
        assertThat(queue.isPaused()).isFalse();

        queue.pause();
        assertThat(queue.isPaused()).isTrue();

        queue.resume();
        assertThat(queue.isPaused()).isFalse();
    }

    @Test
    void stats_backlogExcludesTerminalStates() {
        QueueStats stats = new QueueStats(QueueName.DEFAULT, false, Weight.DEFAULT, 5, 2, 3, 1, 100, 4);

        // Backlog is the number worth alerting on: completed and dead jobs are not work
        // anybody is still waiting for.
        assertThat(stats.backlog()).isEqualTo(11);
        assertThat(stats.total()).isEqualTo(115);
    }

    @Test
    void weight_rejectsOutOfRangeValues() {
        assertThatThrownBy(() -> Weight.of(101)).isInstanceOf(AppError.class);

        // Zero means "unspecified" and resolves to the default rather than failing, so a
        // config that omits the weight still works.
        assertThat(Weight.of(0)).isEqualTo(Weight.DEFAULT);
    }
}
