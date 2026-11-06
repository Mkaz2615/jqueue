package com.jqueue.api.middleware;

import java.io.IOException;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import org.springframework.web.filter.OncePerRequestFilter;

/**
 * Restricts the public port to the client-facing routes.
 *
 * <p>The process listens on two ports. The public one carries the job API that application
 * clients call; the private one carries metrics, health, readiness and the operator
 * dashboard. A cluster can then expose one through an ingress and keep the other on the
 * internal network — which is impossible if a single port serves everything, because then
 * anyone who can enqueue a job can also read the metrics and open the dashboard.
 *
 * <p>Both ports share one application context, so this filter is what keeps them apart:
 * on the public port only {@code /health} and {@code /api/v1/**} exist. Everything else
 * answers 404, exactly as if the route had never been registered there.
 */
public class PublicPortFilter extends OncePerRequestFilter {

    static final String API_PREFIX = "/api/v1/";

    private final int publicPort;

    public PublicPortFilter(int publicPort) {
        this.publicPort = publicPort;
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
            throws ServletException, IOException {
        if (request.getLocalPort() == publicPort && !isPublicRoute(request.getRequestURI())) {
            response.setStatus(HttpServletResponse.SC_NOT_FOUND);
            response.setContentType("application/json");
            response.getWriter().write("{\"code\":\"not_found\",\"message\":\"no route matches "
                    + request.getRequestURI().replace("\"", "") + "\"}");
            return;
        }
        chain.doFilter(request, response);
    }

    static boolean isPublicRoute(String path) {
        return "/health".equals(path) || "/api/v1".equals(path) || path.startsWith(API_PREFIX);
    }
}
