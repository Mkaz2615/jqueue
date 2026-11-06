package com.jqueue.server;

import org.apache.catalina.connector.Connector;
import org.springframework.boot.tomcat.servlet.TomcatServletWebServerFactory;
import org.springframework.boot.web.server.WebServerFactoryCustomizer;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.Ordered;

import com.jqueue.api.middleware.PublicPortFilter;
import com.jqueue.api.middleware.RequestIdFilter;
import com.jqueue.api.middleware.RequestLoggingFilter;
import com.jqueue.common.uid.IdGenerator;
import com.jqueue.infrastructure.config.JQueueProperties;

/**
 * Owns the HTTP layer's shape: the two listeners and the filter chain in front of them.
 *
 * <p>Two ports rather than one is a deployment decision made structural. The public port
 * carries the job API that application clients call; the private port carries metrics,
 * health and the operator dashboard. The primary Tomcat connector serves the public port
 * (Spring's {@code server.port} is bound to {@code jqueue.server.public_port}), and an
 * extra connector added here serves the private one.
 *
 * <p>Filter order matters and is not arbitrary: the request id comes first so every log
 * line below it is correlated, the request logger next so it sees the final status, and the
 * public-port gate last. Exceptions thrown by controllers are mapped by
 * {@link com.jqueue.api.middleware.GlobalExceptionHandler}.
 */
@Configuration(proxyBeanMethods = false)
public class HttpServerConfiguration {

    /** Adds the private listener next to the public one. */
    @Bean
    WebServerFactoryCustomizer<TomcatServletWebServerFactory> privatePortConnector(JQueueProperties props) {
        return factory -> {
            Connector connector = new Connector(TomcatServletWebServerFactory.DEFAULT_PROTOCOL);
            connector.setPort(props.getServer().getPrivatePort());
            // An explicit timeout, because a connector with none will hold a connection open
            // forever for a client that never finishes sending.
            connector.setProperty("connectionTimeout",
                    Long.toString(props.getServer().getReadTimeout().toMillis()));
            factory.addAdditionalConnectors(connector);
        };
    }

    @Bean
    FilterRegistrationBean<RequestIdFilter> requestIdFilter(IdGenerator ids) {
        FilterRegistrationBean<RequestIdFilter> registration = new FilterRegistrationBean<>(new RequestIdFilter(ids));
        registration.setOrder(Ordered.HIGHEST_PRECEDENCE);
        return registration;
    }

    @Bean
    FilterRegistrationBean<RequestLoggingFilter> requestLoggingFilter() {
        FilterRegistrationBean<RequestLoggingFilter> registration = new FilterRegistrationBean<>(new RequestLoggingFilter());
        registration.setOrder(Ordered.HIGHEST_PRECEDENCE + 1);
        return registration;
    }

    @Bean
    FilterRegistrationBean<PublicPortFilter> publicPortFilter(JQueueProperties props) {
        FilterRegistrationBean<PublicPortFilter> registration =
                new FilterRegistrationBean<>(new PublicPortFilter(props.getServer().getPublicPort()));
        registration.setOrder(Ordered.HIGHEST_PRECEDENCE + 2);
        return registration;
    }
}
