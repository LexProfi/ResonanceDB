/*
 * ResonanceDB — Waveform Semantic Engine
 * Copyright © 2025-2026 Aleksandr Listopad
 * SPDX-License-Identifier: LicenseRef-ResonanceDB-License-v1.0
 *
 * Patent notice: The authors intend to seek patent protection for this software.
 * Commercial use >30 days → license@evacortex.ai
 */
package ai.evacortex.resonancedb.server;

import ai.evacortex.resonancedb.core.corpus.CorpusService;
import ai.evacortex.resonancedb.core.storage.FileSystemCorpusService;
import ai.evacortex.resonancedb.core.storage.StoreRuntimeServices;
import ai.evacortex.resonancedb.rest.ResonanceDBRest;

import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

public final class ResonanceDBServer {

    private static final int DEFAULT_PORT = 31415;
    private static final String DEFAULT_DB_ROOT = "./resonancedb-data";

    private ResonanceDBServer() {}

    public static void main(String[] args) {
        Map<String, String> a = parseArgs(args);

        Path dbRoot = Path.of(Objects.requireNonNull(firstNonBlank(
                a.get("db"),
                System.getenv("RESONANCE_DB_ROOT"),
                DEFAULT_DB_ROOT
        ))).toAbsolutePath().normalize();

        int port = clampPort(parseInt(firstNonBlank(
                a.get("port"),
                System.getenv("RESONANCE_SERVER_PORT"),
                String.valueOf(DEFAULT_PORT)
        ), DEFAULT_PORT));

        String maxBody = firstNonBlank(
                a.get("maxBodyBytes"),
                System.getenv("RESONANCE_REST_MAX_BODY_BYTES")
        );
        if (maxBody != null) {
            System.setProperty("resonance.rest.maxBodyBytes", maxBody.trim());
        }

        showSystemInfo(dbRoot, port);

        AtomicBoolean stopped = new AtomicBoolean(false);

        try (StoreRuntimeServices runtime = StoreRuntimeServices.fromSystemProperties();
             CorpusService corpora = new FileSystemCorpusService(dbRoot, runtime, false);
             ResonanceDBRest rest = new ResonanceDBRest(corpora, port)) {

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                if (!stopped.compareAndSet(false, true)) {
                    return;
                }
                try {
                    rest.stop(0);
                } catch (Exception ignored) {
                }
            }, "resonancedb-server-shutdown"));

            rest.start();

            System.out.println("[resonance-server] started");
            System.out.println("[resonance-server] dbRoot=" + dbRoot);
            System.out.println("[resonance-server] port=" + port);

            for (;;) {
                try {
                    Thread.sleep(60_000L);
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
        return (port >= 1 && port <= 65_535) ? port : DEFAULT_PORT;
    }

    private static void showSystemInfo(Path dbRoot, int port) {
        System.out.printf(
                "Started at: %s%n",
                LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
        );
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
        if (maxBody != null) {
            System.out.println("REST maxBodyBytes: " + maxBody);
        }

        System.out.println("Corpus mode: enabled");
        System.out.println("Default corpus id: " + System.getProperty("resonance.corpus.defaultId", "default"));
        System.out.println("Legacy default corpus mapping: " +
                Boolean.parseBoolean(System.getProperty("resonance.corpus.legacy.default.enabled", "true")));
        System.out.println("Max open corpora: " +
                Integer.getInteger(
                        "resonance.corpus.maxOpen",
                        Math.max(32, Runtime.getRuntime().availableProcessors() * 4)
                ));
        System.out.println("Idle corpus close (sec): " +
                Long.getLong("resonance.corpus.idleClose.seconds", 600L));
        System.out.println("Pattern length: " +
                Integer.getInteger("resonance.pattern.len", 1536));
        System.out.println("Native kernel: " +
                Boolean.parseBoolean(System.getProperty("resonance.kernel.native", "false")));
        System.out.println("Async flush: " +
                Boolean.parseBoolean(System.getProperty("resonance.flush.async", "false")));
        System.out.println("Flush interval (ms): " +
                Long.getLong("resonance.flush.interval.millis", 5L));
    }

    private static Map<String, String> parseArgs(String[] args) {
        Map<String, String> out = new HashMap<>();
        if (args == null) {
            return out;
        }

        for (String s : args) {
            if (s == null) {
                continue;
            }

            String a = s.trim();
            if (a.isEmpty()) {
                continue;
            }

            if (a.startsWith("--")) {
                a = a.substring(2);
            }

            int eq = a.indexOf('=');
            if (eq > 0) {
                out.put(a.substring(0, eq).trim(), a.substring(eq + 1).trim());
            } else {
                out.put(a, "true");
            }
        }
        return out;
    }

    private static String firstNonBlank(String... xs) {
        for (String x : xs) {
            if (x != null) {
                String t = x.trim();
                if (!t.isEmpty()) {
                    return t;
                }
            }
        }
        return null;
    }

    private static int parseInt(String s, int def) {
        if (s == null) {
            return def;
        }
        try {
            return Integer.parseInt(s.trim());
        } catch (NumberFormatException e) {
            return def;
        }
    }

    private static String safeMsg(Throwable t) {
        String m = t.getMessage();
        return (m == null || m.isBlank()) ? t.getClass().getSimpleName() : m;
    }
}