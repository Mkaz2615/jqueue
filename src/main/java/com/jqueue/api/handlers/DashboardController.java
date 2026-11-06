package com.jqueue.api.handlers;

import org.springframework.http.CacheControl;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import com.jqueue.infrastructure.web.DashboardPage;

/**
 * Serves the operator dashboard.
 *
 * <p>The dashboard is a single bundled HTML page that talks to the same REST API a client
 * would use. That is deliberate: there is no dashboard-only endpoint, so every action the
 * UI can take is one an operator can also script, and the UI cannot drift ahead of the API.
 */
@RestController
public class DashboardController {

    public static final String PATH = "/dashboard";

    private final DashboardPage page;

    public DashboardController(DashboardPage page) {
        this.page = page;
    }

    /**
     * The page is rebuilt from the API on every load, so caching it would only serve a
     * stale shell after a deploy.
     */
    @GetMapping(PATH)
    public ResponseEntity<String> index() {
        return ResponseEntity.ok()
                .contentType(new MediaType(MediaType.TEXT_HTML, java.nio.charset.StandardCharsets.UTF_8))
                .cacheControl(CacheControl.noStore())
                .body(page.html());
    }

    @GetMapping("/")
    public ResponseEntity<Void> root() {
        return ResponseEntity.status(HttpStatus.FOUND).header(HttpHeaders.LOCATION, PATH).build();
    }
}
