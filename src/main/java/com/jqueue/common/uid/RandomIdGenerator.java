package com.jqueue.common.uid;

import java.security.SecureRandom;
import java.util.HexFormat;

/** Produces 128-bit random identifiers rendered as 32 lowercase hex characters. */
public final class RandomIdGenerator implements IdGenerator {

    private static final HexFormat HEX = HexFormat.of();

    private final SecureRandom random = new SecureRandom();

    @Override
    public String newId() {
        byte[] buf = new byte[16];
        random.nextBytes(buf);
        return HEX.formatHex(buf);
    }
}
