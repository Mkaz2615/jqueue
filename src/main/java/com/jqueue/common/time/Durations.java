package com.jqueue.common.time;

import java.math.BigDecimal;
import java.time.Duration;

/**
 * Renders durations in the compact form used by Go and by most operations tooling:
 * {@code 500ms}, {@code 30s}, {@code 1m30s}, {@code 24h0m0s}.
 *
 * <p>{@link Duration#toString()} produces ISO-8601 ({@code PT1M30S}), which is precise but
 * awkward to read in a log line or an error message. The configuration file is written in
 * this compact form too, so using it on the way out keeps input and output consistent.
 */
public final class Durations {

    private Durations() {
    }

    /** Formats a duration, for example {@code 1h2m3.5s}. */
    public static String format(Duration duration) {
        if (duration == null || duration.isZero()) {
            return "0s";
        }
        boolean negative = duration.isNegative();
        Duration d = duration.abs();
        String body;

        if (d.compareTo(Duration.ofSeconds(1)) < 0) {
            long nanos = d.toNanos();
            if (nanos < 1_000) {
                body = nanos + "ns";
            } else if (nanos < 1_000_000) {
                body = trim(BigDecimal.valueOf(nanos).movePointLeft(3)) + "µs";
            } else {
                body = trim(BigDecimal.valueOf(nanos).movePointLeft(6)) + "ms";
            }
        } else {
            long hours = d.toHours();
            int minutes = d.toMinutesPart();
            BigDecimal seconds = BigDecimal.valueOf(d.toSecondsPart())
                    .add(BigDecimal.valueOf(d.toNanosPart()).movePointLeft(9));
            StringBuilder sb = new StringBuilder();
            if (hours > 0) {
                sb.append(hours).append('h').append(minutes).append('m');
            } else if (minutes > 0) {
                sb.append(minutes).append('m');
            }
            sb.append(trim(seconds)).append('s');
            body = sb.toString();
        }
        return negative ? "-" + body : body;
    }

    private static String trim(BigDecimal value) {
        BigDecimal stripped = value.stripTrailingZeros();
        return stripped.scale() < 0 ? stripped.setScale(0).toPlainString() : stripped.toPlainString();
    }
}
