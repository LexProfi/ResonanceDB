/*
 * ResonanceDB — Waveform Semantic Engine
 * Copyright © 2025-2026 Aleksandr Listopad
 * SPDX-License-Identifier: LicenseRef-ResonanceDB-License-v1.0
 *
 * Patent notice: The authors intend to seek patent protection for this software.
 * Commercial use >30 days → license@evacortex.ai
 */
package ai.evacortex.resonancedb.server;

import ai.evacortex.resonancedb.core.storage.WavePatternStoreImpl;
import ai.evacortex.resonancedb.rest.ResonanceDBRest;

import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public final class ResonanceDBServer {

    private ResonanceDBServer() {}

    public static void main(String[] args) {
        Map<String, String> a = parseArgs(args);

        Path dbRoot = Path.of(Objects.requireNonNull(firstNonBlank(
                a.get("db"),
                System.getenv("RESONANCE_DB_ROOT"),
                "./resonancedb-data"
        ))).toAbsolutePath().normalize();

        int port = clampPort(parseInt(firstNonBlank(
                a.get("port"),
                System.getenv("RESONANCE_SERVER_PORT"),
                "31415"
        ), 31415));

        String maxBody = firstNonBlank(
                a.get("maxBodyBytes"),
                System.getenv("RESONANCE_REST_MAX_BODY_BYTES")
        );
        if (maxBody != null) {
            System.setProperty("resonance.rest.maxBodyBytes", maxBody.trim());
        }

        showSystemInfo(dbRoot, port);

        final java.util.concurrent.atomic.AtomicBoolean stopped = new java.util.concurrent.atomic.AtomicBoolean(false);

        try (WavePatternStoreImpl store = new WavePatternStoreImpl(dbRoot);
             ResonanceDBRest rest = new ResonanceDBRest(store, store, port)) {

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                if (!stopped.compareAndSet(false, true)) return;
                try { rest.stop(0); } catch (Exception ignored) {}
            }, "resonancedb-server-shutdown"));

            rest.start();
            System.out.println("[resonance-server] started");
            System.out.println("[resonance-server] dbRoot=" + dbRoot);
            System.out.println("[resonance-server] port=" + port);
            for (;;) {
                try {
                    Thread.sleep(60_000);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }

        } catch (Throwable t) {
            System.err.println("[resonance-server] failed to start: " + safeMsg(t));
            t.printStackTrace(System.err);
            System.exit(1);
        }
    }

    private static int clampPort(int port) {
        if (port >= 1 && port <= 65_535) return port;
        return 31415;
    }

    private static void showSystemInfo(Path dbRoot, int port) {
        System.out.printf("Started at: %s%n", LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
        System.out.printf("OS   : %s %s%n", System.getProperty("os.name"), System.getProperty("os.version"));
        System.out.printf("CPU  : %d logical cores%n", Runtime.getRuntime().availableProcessors());
        System.out.printf("JVM  : %s %s%n", System.getProperty("java.vm.name"), System.getProperty("java.version"));
        var rt = java.lang.management.ManagementFactory.getRuntimeMXBean();
        System.out.println("JVM args: " + rt.getInputArguments());
        System.out.println("Heap: max=" + (Runtime.getRuntime().maxMemory() / (1024 * 1024)) + " MB");
        System.out.println("DB  : " + dbRoot);
        System.out.println("Port: " + port);
        System.out.println("HTTP executor: virtual");

        String maxBody = System.getProperty("resonance.rest.maxBodyBytes");
        if (maxBody != null) System.out.println("REST maxBodyBytes: " + maxBody);
    }

    private static Map<String, String> parseArgs(String[] args) {
        Map<String, String> out = new HashMap<>();
        if (args == null) return out;

        for (String s : args) {
            if (s == null) continue;
            String a = s.trim();
            if (a.isEmpty()) continue;

            if (a.startsWith("--")) a = a.substring(2);
            int eq = a.indexOf('=');
            if (eq > 0) out.put(a.substring(0, eq).trim(), a.substring(eq + 1).trim());
            else out.put(a.trim(), "true");
        }
        return out;
    }

    private static String firstNonBlank(String... xs) {
        for (String x : xs) {
            if (x != null) {
                String t = x.trim();
                if (!t.isEmpty()) return t;
            }
        }
        return null;
    }

    private static int parseInt(String s, int def) {
        if (s == null) return def;
        try { return Integer.parseInt(s.trim()); }
        catch (NumberFormatException e) { return def; }
    }

    private static String safeMsg(Throwable t) {
        String m = t.getMessage();
        if (m == null || m.isBlank()) return t.getClass().getSimpleName();
        return m;
    }
}