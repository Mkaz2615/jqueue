package com.jqueue.integration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ConfigurableApplicationContext;

import com.jqueue.JQueueApplication;
import com.jqueue.common.uid.RandomIdGenerator;
import com.jqueue.infrastructure.redis.RedisClientFactory;
import redis.clients.jedis.UnifiedJedis;

/**
 * Boots the whole service against a real Redis and drives it over HTTP: both listeners,
 * the worker pool, the scheduler and the metrics endpoint, wired exactly as in production.
 */
@Tag("integration")
class JQueueApplicationIT {

    static final int PUBLIC_PORT = 18080;
    static final int PRIVATE_PORT = 19090;

    static ConfigurableApplicationContext context;
    static final HttpClient http = HttpClient.newHttpClient();

    @BeforeAll
    static void boot() {
        String addr = RedisBrokerIT.redisAddr();
        try (UnifiedJedis probe = RedisClientFactory.create(new RedisClientFactory.ClientConfig(
                List.of(addr), "", "", 0, 1, 0, Duration.ofSeconds(2), Duration.ofSeconds(2), Duration.ofSeconds(2)))) {
            probe.ping();
        } catch (RuntimeException e) {
            assumeTrue(false, "redis is not reachable at " + addr);
        }

        String namespace = "jqueue-app-test-" + new RandomIdGenerator().newId().substring(0, 8);
        context = new SpringApplication(JQueueApplication.class).run(
                "--spring.config.import=",
                "--jqueue.server.public_port=" + PUBLIC_PORT,
                "--jqueue.server.private_port=" + PRIVATE_PORT,
                "--jqueue.redis.addrs=" + addr,
                "--jqueue.redis.namespace=" + namespace,
                "--jqueue.worker.poll_interval=20ms",
                "--jqueue.scheduler.promote_interval=100ms",
                "--jqueue.scheduler.metrics_interval=100ms",
                "--logging.level.root=warn");
    }

    @AfterAll
    static void shutdown() {
        if (context != null) {
            context.close();
        }
    }

    static HttpResponse<String> send(HttpRequest.Builder request) throws Exception {
        return http.send(request.build(), HttpResponse.BodyHandlers.ofString());
    }

    static HttpRequest.Builder at(int port, String path) {
        return HttpRequest.newBuilder(URI.create("http://localhost:" + port + path));
    }

    static String field(String json, String name) {
        Matcher m = Pattern.compile("\"" + name + "\":\"([^\"]*)\"").matcher(json);
        return m.find() ? m.group(1) : null;
    }

    @Test
    void aJobEnqueuedOverHttpIsProcessedByTheWorkerPool() throws Exception {
        HttpResponse<String> created = send(at(PUBLIC_PORT, "/api/v1/jobs")
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(
                        "{\"task_type\":\"email.send\",\"queue\":\"critical\",\"payload\":{\"to\":\"a@b.c\"}}")));

        assertThat(created.statusCode()).isEqualTo(201);
        String id = field(created.body(), "id");

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            HttpResponse<String> job = send(at(PUBLIC_PORT, "/api/v1/jobs/" + id).GET());
            assertThat(field(job.body(), "state")).isEqualTo("completed");
        });

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            String metrics = send(at(PRIVATE_PORT, "/metrics").GET()).body();
            assertThat(metrics).contains(
                    "jqueue_jobs_processed_total{queue=\"critical\",status=\"success\",task_type=\"email.send\"} 1.0");
            assertThat(metrics).contains("jqueue_queue_depth{queue=\"critical\",state=\"completed\"} 1.0");
        });
    }

    @Test
    void aDelayedJobIsPromotedAndRun() throws Exception {
        HttpResponse<String> created = send(at(PUBLIC_PORT, "/api/v1/jobs")
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(
                        "{\"task_type\":\"email.send\",\"delay_seconds\":1,\"payload\":{\"to\":\"later@b.c\"}}")));
        assertThat(field(created.body(), "state")).isEqualTo("scheduled");
        String id = field(created.body(), "id");

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertThat(field(send(at(PUBLIC_PORT, "/api/v1/jobs/" + id).GET()).body(), "state"))
                        .isEqualTo("completed"));
    }

    @Test
    void privateRoutesAreServedOnlyOnThePrivatePort() throws Exception {
        assertThat(send(at(PUBLIC_PORT, "/health").GET()).statusCode()).isEqualTo(200);
        assertThat(send(at(PUBLIC_PORT, "/metrics").GET()).statusCode()).isEqualTo(404);
        assertThat(send(at(PUBLIC_PORT, "/dashboard").GET()).statusCode()).isEqualTo(404);
        assertThat(send(at(PUBLIC_PORT, "/ready").GET()).statusCode()).isEqualTo(404);

        assertThat(send(at(PRIVATE_PORT, "/ready").GET()).statusCode()).isEqualTo(200);
        assertThat(send(at(PRIVATE_PORT, "/dashboard").GET()).body()).contains("<title>JQueue</title>");
        // The dashboard calls the API on its own port, so the API is mounted there too.
        assertThat(send(at(PRIVATE_PORT, "/api/v1/queues").GET()).statusCode()).isEqualTo(200);
    }
}
