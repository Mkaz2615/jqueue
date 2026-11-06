package com.jqueue;

import java.util.LinkedHashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.MapPropertySource;

/**
 * Runs the JQueue service: the HTTP API, the worker pool, the periodic sweeps, or any
 * combination of them.
 *
 * <p>One jar serves every role, selected by configuration. A deployment can run API-only
 * pods behind a load balancer and worker-only pods on a different node pool, from the same
 * image, with no build-time variants to keep in sync.
 *
 * <p>Component scanning is limited to the HTTP and wiring packages. The domain, application
 * and infrastructure classes are plain Java with no framework annotations; they are
 * constructed explicitly in {@link com.jqueue.di.JQueueConfiguration}.
 */
@SpringBootApplication(scanBasePackages = {"com.jqueue.api", "com.jqueue.server", "com.jqueue.di"})
public class JQueueApplication {

    private static final Logger log = LoggerFactory.getLogger(JQueueApplication.class);

    /**
     * Environment variables that do not follow Spring's automatic {@code JQUEUE_SECTION_KEY}
     * mapping. They are kept for compatibility with the original deployment manifests.
     */
    static final Map<String, String> ENV_ALIASES = Map.of(
            "JQUEUE_PUBLIC_PORT", "jqueue.server.public_port",
            "JQUEUE_PRIVATE_PORT", "jqueue.server.private_port",
            "JQUEUE_LOG_LEVEL", "jqueue.logger.level");

    public static void main(String[] args) {
        SpringApplication app = new SpringApplication(JQueueApplication.class);
        app.addInitializers(context -> applyEnvAliases(context.getEnvironment()));
        app.addListeners(new LifecycleLogger());
        app.run(args);
    }

    /** Overlays the alias variables with the highest precedence, as the environment should have. */
    static void applyEnvAliases(ConfigurableEnvironment environment) {
        Map<String, Object> overrides = new LinkedHashMap<>();
        ENV_ALIASES.forEach((variable, property) -> {
            String value = System.getenv(variable);
            if (value != null && !value.isEmpty()) {
                overrides.put(property, value);
            }
        });
        if (!overrides.isEmpty()) {
            environment.getPropertySources().addFirst(new MapPropertySource("jqueueEnvAliases", overrides));
        }
    }

    /** Logs the service's start and stop with the facts an operator looks for first. */
    static final class LifecycleLogger implements ApplicationListener<org.springframework.context.ApplicationEvent> {
        @Override
        public void onApplicationEvent(org.springframework.context.ApplicationEvent event) {
            if (event instanceof ApplicationReadyEvent ready) {
                var env = ready.getApplicationContext().getEnvironment();
                String publicPort = env.getProperty("jqueue.server.public_port", "8080");
                String privatePort = env.getProperty("jqueue.server.private_port", "9090");
                log.atInfo()
                        .addKeyValue("env", env.getProperty("jqueue.env", "local"))
                        .addKeyValue("version", env.getProperty("jqueue.server.version", "1.0.0"))
                        .addKeyValue("worker_enabled", env.getProperty("jqueue.worker.enabled", "true"))
                        .addKeyValue("scheduler_enabled", env.getProperty("jqueue.scheduler.enabled", "true"))
                        .addKeyValue("api", "http://localhost:" + publicPort + "/api/v1")
                        .addKeyValue("dashboard", "http://localhost:" + privatePort + "/dashboard")
                        .addKeyValue("metrics", "http://localhost:" + privatePort + "/metrics")
                        .log("jqueue is ready");
            } else if (event instanceof ContextClosedEvent) {
                log.info("shutdown signal received; draining");
            }
        }
    }
}
