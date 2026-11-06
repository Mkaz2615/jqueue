package com.jqueue.infrastructure.web;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;

/**
 * The single-page operator dashboard, bundled inside the jar.
 *
 * <p>Bundling rather than serving from disk means the jar is the whole deployment: no
 * sidecar, no volume mount, no version skew between the API and the UI that calls it. The
 * page is read once at startup.
 */
public final class DashboardPage {

    private static final String RESOURCE = "/web/dashboard.html";

    private final String html;

    public DashboardPage() {
        try (InputStream in = DashboardPage.class.getResourceAsStream(RESOURCE)) {
            if (in == null) {
                throw new IllegalStateException("dashboard page is missing from the classpath: " + RESOURCE);
            }
            this.html = new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException("failed to read the dashboard page", e);
        }
    }

    public String html() {
        return html;
    }
}
