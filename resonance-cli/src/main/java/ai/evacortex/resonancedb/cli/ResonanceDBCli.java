/*
 * ResonanceDB — Waveform Semantic Engine
 * Copyright © 2025-2026 Aleksandr Listopad
 * SPDX-License-Identifier: LicenseRef-ResonanceDB-License-v1.0
 *
 * Patent notice: The authors intend to seek patent protection for this software.
 * Commercial use >30 days → license@evacortex.ai
 */
package ai.evacortex.resonancedb.cli;

import ai.evacortex.resonancedb.core.ResonanceStore;
import ai.evacortex.resonancedb.core.storage.WavePattern;
import ai.evacortex.resonancedb.core.storage.WavePatternStoreImpl;
import ai.evacortex.resonancedb.core.storage.responce.InterferenceEntry;
import ai.evacortex.resonancedb.core.storage.responce.InterferenceMap;
import ai.evacortex.resonancedb.core.storage.responce.ResonanceMatch;
import ai.evacortex.resonancedb.core.storage.responce.ResonanceMatchDetailed;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.util.*;

/**
 * Minimal CLI wrapper for ResonanceDB.
 *
 * Examples:
 *  java -jar resonance-cli.jar --db=./data info
 *  java -jar resonance-cli.jar --db=./data compare --ampA=1,2,3 --phaseA=0,0.1,0.2 --ampB=1,2,3 --phaseB=0,0.1,0.2
 *  java -jar resonance-cli.jar --db=./data insert --amp=1,2,3 --phase=0,0.1,0.2 --meta=key:value,foo:bar
 *  java -jar resonance-cli.jar --db=./data query --amp=1,2,3 --phase=0,0.1,0.2 --topK=10
 *  java -jar resonance-cli.jar --db=./data repl
 *
 * Notes:
 *  - This CLI intentionally keeps dependencies at zero (no picocli/jcommander).
 *  - WavePattern is passed as comma-separated doubles.
 */
public final class ResonanceDBCli {

    private ResonanceDBCli() {}

    public static void main(String[] args) {
        Map<String, String> flags = parseFlags(args);
        List<String> rest = parsePositional(args);

        String cmd = rest.isEmpty() ? "help" : rest.getFirst().toLowerCase(Locale.ROOT);
        boolean debug = isTrue(flags.get("debug"));

        if (isHelp(cmd)) {
            printHelp();
            return;
        }

        Path dbRoot = resolveDbRoot(flags);

        try (WavePatternStoreImpl store = new WavePatternStoreImpl(dbRoot)) {
            int code = dispatch(cmd, dbRoot, store, flags);
            System.exit(code);
        } catch (Exception e) {
            System.err.println("ERR: " + safeMsg(e));
            if (debug) e.printStackTrace(System.err);
            System.exit(1);
        }
    }

    private static int dispatch(String cmd, Path dbRoot, WavePatternStoreImpl store, Map<String, String> flags)
            throws Exception {

        return switch (cmd) {
            case "info" -> { cmdInfo(dbRoot, store); yield 0; }
            case "compare" -> { cmdCompare(store, flags); yield 0; }
            case "insert" -> { cmdInsert(store, flags); yield 0; }
            case "delete" -> { cmdDelete(store, flags); yield 0; }
            case "replace" -> { cmdReplace(store, flags); yield 0; }
            case "query" -> { cmdQuery(store, flags); yield 0; }
            case "querydetailed" -> { cmdQueryDetailed(store, flags); yield 0; }
            case "interference" -> { cmdInterference(store, flags); yield 0; }
            case "interferencemap" -> { cmdInterferenceMap(store, flags); yield 0; }
            case "composite" -> { cmdComposite(store, flags); yield 0; }
            case "compositedetailed" -> { cmdCompositeDetailed(store, flags); yield 0; }
            case "repl" -> { repl(dbRoot, store); yield 0; }
            default -> {
                System.err.println("ERR: Unknown command: " + cmd);
                printHelp();
                yield 2;
            }
        };
    }

    private static boolean isHelp(String cmd) {
        return "help".equals(cmd) || "-h".equals(cmd) || "--help".equals(cmd);
    }

    private static void cmdInfo(Path dbRoot, WavePatternStoreImpl store) {
        System.out.println("ResonanceDB CLI");
        System.out.println("dbRoot: " + dbRoot);
        System.out.println("kernel: " + (Boolean.getBoolean("resonance.kernel.native") ? "NativeKernel" : "JavaKernel"));
        System.out.println("heapMaxMB: " + (Runtime.getRuntime().maxMemory() / (1024 * 1024)));
        System.out.println("processors: " + Runtime.getRuntime().availableProcessors());
        try {
            var selector = store.getShardSelector();
            System.out.println("shards: " + (selector == null ? "n/a" : "ready"));
        } catch (Throwable ignored) {
            System.out.println("shards: n/a");
        }
    }

    private static void cmdCompare(ResonanceStore store, Map<String, String> flags) {
        WavePattern a = readPattern(flags, "a", "ampA", "phaseA");
        WavePattern b = readPattern(flags, "b", "ampB", "phaseB");
        float score = store.compare(a, b);
        System.out.println(score);
    }

    private static void cmdInsert(ResonanceStore store, Map<String, String> flags) {
        WavePattern p = readPattern(flags, null, "amp", "phase");
        Map<String, String> meta = parseMeta(flags.get("meta"));
        String id = store.insert(p, meta);
        System.out.println(id);
    }

    private static void cmdDelete(ResonanceStore store, Map<String, String> flags) {
        String id = require(flags, "id");
        store.delete(id);
        System.out.println("ok");
    }

    private static void cmdReplace(ResonanceStore store, Map<String, String> flags) {
        String id = require(flags, "id");
        WavePattern p = readPattern(flags, null, "amp", "phase");
        Map<String, String> meta = parseMeta(flags.get("meta"));
        String newId = store.replace(id, p, meta);
        System.out.println(newId);
    }

    private static void cmdQuery(ResonanceStore store, Map<String, String> flags) {
        WavePattern q = readPattern(flags, null, "amp", "phase");
        int topK = parseIntOr(flags, "topk", 10);
        List<ResonanceMatch> matches = store.query(q, clampTopK(topK));
        printMatches(matches);
    }

    private static void cmdQueryDetailed(ResonanceStore store, Map<String, String> flags) {
        WavePattern q = readPattern(flags, null, "amp", "phase");
        int topK = parseIntOr(flags, "topk", 10);
        List<ResonanceMatchDetailed> matches = store.queryDetailed(q, clampTopK(topK));
        printMatchesDetailed(matches);
    }

    private static void cmdInterference(ResonanceStore store, Map<String, String> flags) {
        WavePattern q = readPattern(flags, null, "amp", "phase");
        int topK = parseIntOr(flags, "topk", 10);
        InterferenceMap map = store.queryInterference(q, clampTopK(topK));
        System.out.println(map);
    }

    private static void cmdInterferenceMap(ResonanceStore store, Map<String, String> flags) {
        WavePattern q = readPattern(flags, null, "amp", "phase");
        int topK = parseIntOr(flags, "topk", 10);
        List<InterferenceEntry> list = store.queryInterferenceMap(q, clampTopK(topK));
        for (InterferenceEntry e : list) {
            System.out.printf(Locale.ROOT, "%s\t%.8f\t%.8f\t%s%n",
                    e.id(), e.energy(), e.phaseShift(), String.valueOf(e.zone()));
        }
    }

    private static void cmdComposite(ResonanceStore store, Map<String, String> flags) {
        CompositeInput in = readComposite(flags);
        int topK = parseIntOr(flags, "topk", 10);
        List<ResonanceMatch> matches = store.queryComposite(in.patterns, in.weights, clampTopK(topK));
        printMatches(matches);
    }

    private static void cmdCompositeDetailed(ResonanceStore store, Map<String, String> flags) {
        CompositeInput in = readComposite(flags);
        int topK = parseIntOr(flags, "topk", 10);
        List<ResonanceMatchDetailed> matches = store.queryCompositeDetailed(in.patterns, in.weights, clampTopK(topK));
        printMatchesDetailed(matches);
    }

    private static void repl(Path dbRoot, WavePatternStoreImpl store) throws IOException {
        System.out.println("ResonanceDB REPL");
        System.out.println("dbRoot: " + dbRoot);
        System.out.println("Type 'help' for commands, 'exit' to quit.");

        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        while (true) {
            System.out.print("> ");
            String line = br.readLine();
            if (line == null) break;

            line = line.trim();
            if (line.isEmpty()) continue;

            if ("exit".equalsIgnoreCase(line) || "quit".equalsIgnoreCase(line)) break;
            if ("help".equalsIgnoreCase(line)) {
                printReplHelp();
                continue;
            }

            try {
                runReplLine(store, line);
            } catch (Exception e) {
                System.err.println("ERR: " + safeMsg(e));
            }
        }
        System.out.println("bye");
    }

    private static void runReplLine(ResonanceStore store, String line) {
        String[] parts = splitArgs(line);
        if (parts.length == 0) return;

        String cmd = parts[0].toLowerCase(Locale.ROOT);
        Map<String, String> flags = new HashMap<>();

        for (int i = 1; i < parts.length; i++) {
            String p = parts[i];
            if (p == null) continue;
            p = p.trim();
            if (p.isEmpty()) continue;

            if (p.startsWith("--")) p = p.substring(2);
            int eq = p.indexOf('=');
            if (eq > 0) {
                flags.put(p.substring(0, eq).trim().toLowerCase(Locale.ROOT), p.substring(eq + 1).trim());
            } else {
                flags.put(p.trim().toLowerCase(Locale.ROOT), "true");
            }
        }

        switch (cmd) {
            case "compare" -> cmdCompare(store, flags);
            case "insert" -> cmdInsert(store, flags);
            case "delete" -> cmdDelete(store, flags);
            case "replace" -> cmdReplace(store, flags);
            case "query" -> cmdQuery(store, flags);
            case "querydetailed" -> cmdQueryDetailed(store, flags);
            case "interference" -> cmdInterference(store, flags);
            case "interferencemap" -> cmdInterferenceMap(store, flags);
            case "composite" -> cmdComposite(store, flags);
            case "compositedetailed" -> cmdCompositeDetailed(store, flags);
            default -> System.err.println("ERR: Unknown REPL command: " + cmd);
        }
    }

    private static WavePattern readPattern(Map<String, String> flags, String prefix, String ampKey, String phaseKey) {
        String amp = null;
        String phs = null;

        if (prefix != null) {
            if ("a".equals(prefix)) {
                amp = firstNonBlank(amp, flags.get("ampa"));
                phs = firstNonBlank(phs, flags.get("phasea"));
            } else if ("b".equals(prefix)) {
                amp = firstNonBlank(amp, flags.get("ampb"));
                phs = firstNonBlank(phs, flags.get("phaseb"));
            }
        }

        if (prefix != null) {
            amp = firstNonBlank(amp, flags.get((prefix + ".amp").toLowerCase(Locale.ROOT)));
            phs = firstNonBlank(phs, flags.get((prefix + ".phase").toLowerCase(Locale.ROOT)));
        }

        amp = firstNonBlank(amp, flags.get(ampKey.toLowerCase(Locale.ROOT)), flags.get(ampKey));
        phs = firstNonBlank(phs, flags.get(phaseKey.toLowerCase(Locale.ROOT)), flags.get(phaseKey));

        if (amp == null || phs == null) {
            throw new IllegalArgumentException("WavePattern requires --" + ampKey + " and --" + phaseKey);
        }

        double[] A = parseDoubles(amp, "--" + ampKey);
        double[] P = parseDoubles(phs, "--" + phaseKey);

        if (A.length == 0 || P.length == 0) throw new IllegalArgumentException("Empty amplitude/phase");
        if (A.length != P.length) throw new IllegalArgumentException("Amplitude/phase length mismatch");

        return new WavePattern(A, P);
    }

    private static CompositeInput readComposite(Map<String, String> flags) {
        String patternsRaw = firstNonBlank(flags.get("patterns"), flags.get("p"));
        if (patternsRaw == null) {
            throw new IllegalArgumentException("Composite requires --patterns (format: amp|phase; amp|phase; ...)");
        }

        List<WavePattern> patterns = new ArrayList<>();
        for (String block : patternsRaw.split(";")) {
            String b = block.trim();
            if (b.isEmpty()) continue;

            int bar = b.indexOf('|');
            if (bar <= 0) throw new IllegalArgumentException("Bad pattern block: " + b + " (expected amp|phase)");

            String amp = b.substring(0, bar).trim();
            String phs = b.substring(bar + 1).trim();

            double[] A = parseDoubles(amp, "composite amp");
            double[] P = parseDoubles(phs, "composite phase");
            if (A.length != P.length) throw new IllegalArgumentException("Amplitude/phase mismatch in block: " + b);

            patterns.add(new WavePattern(A, P));
        }
        if (patterns.isEmpty()) throw new IllegalArgumentException("Composite patterns list is empty");

        List<Double> weights = null;
        String wRaw = firstNonBlank(flags.get("weights"), flags.get("w"));
        if (wRaw != null && !wRaw.trim().isEmpty()) {
            weights = new ArrayList<>();
            for (String s : wRaw.split(",")) {
                String t = s.trim();
                if (t.isEmpty()) continue;
                try {
                    weights.add(Double.parseDouble(t));
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException("Bad weight: " + t);
                }
            }
        }

        return new CompositeInput(patterns, weights);
    }

    private static Map<String, String> parseMeta(String meta) {
        if (meta == null || meta.trim().isEmpty()) return Map.of();

        Map<String, String> out = new LinkedHashMap<>();
        for (String pair : meta.split(",")) {
            String p = pair.trim();
            if (p.isEmpty()) continue;

            int c = p.indexOf(':');
            if (c <= 0) throw new IllegalArgumentException("Bad meta pair: " + p + " (expected key:value)");

            String k = p.substring(0, c).trim();
            String v = p.substring(c + 1).trim();
            if (k.isEmpty()) throw new IllegalArgumentException("Bad meta pair: " + p + " (empty key)");
            out.put(k, v);
        }
        return out;
    }

    private static double[] parseDoubles(String csv, String what) {
        String s = (csv == null) ? "" : csv.trim();
        if (s.isEmpty()) return new double[0];

        String[] parts = s.split(",");
        double[] out = new double[parts.length];

        for (int i = 0; i < parts.length; i++) {
            String t = parts[i].trim();
            if (t.isEmpty()) throw new IllegalArgumentException("Bad double in " + what + ": empty item");
            try {
                out[i] = Double.parseDouble(t);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Bad double in " + what + ": " + t);
            }
        }
        return out;
    }

    private static int clampTopK(int k) {
        if (k <= 0) return 0;
        return Math.min(k, 10_000);
    }

    private static void printMatches(List<ResonanceMatch> matches) {
        for (ResonanceMatch m : matches) {
            System.out.printf(Locale.ROOT, "%s\t%.8f%n", m.id(), m.energy());
        }
    }

    private static void printMatchesDetailed(List<ResonanceMatchDetailed> matches) {
        for (ResonanceMatchDetailed m : matches) {
            System.out.printf(Locale.ROOT, "%s\t%.8f\t%.8f\t%s\t%.4f%n",
                    m.id(), m.energy(), m.phaseDelta(), String.valueOf(m.zone()), m.zoneScore());
        }
    }

    private static void printHelp() {
        System.out.println("""
                ResonanceDB CLI

                Usage:
                  java -jar resonance-cli.jar --db=PATH <command> [--k=v ...]

                Commands:
                  info
                  compare --ampA=... --phaseA=... --ampB=... --phaseB=...
                  insert  --amp=...  --phase=... [--meta=key:value,foo:bar]
                  delete  --id=<patternId>
                  replace --id=<oldId> --amp=... --phase=... [--meta=...]
                  query   --amp=... --phase=... [--topK=10]
                  queryDetailed --amp=... --phase=... [--topK=10]
                  interference --amp=... --phase=... [--topK=10]
                  interferenceMap --amp=... --phase=... [--topK=10]
                  composite --patterns="amp|phase; amp|phase; ..." [--weights=1,0.5,0.2] [--topK=10]
                  compositeDetailed --patterns="amp|phase; amp|phase; ..." [--weights=...] [--topK=10]
                  repl

                Flags:
                  --db=PATH
                  --debug=true   (prints stacktraces on errors)

                Notes:
                  - Vectors are comma-separated doubles. Example: --amp=1,0.5,0.2 --phase=0,0.1,-0.2
                  - composite patterns format: "amp|phase; amp|phase; ..."
                """);
    }

    private static void printReplHelp() {
        System.out.println("""
                REPL commands:
                  compare --ampA=... --phaseA=... --ampB=... --phaseB=...
                  insert --amp=... --phase=... [--meta=key:value,...]
                  delete --id=...
                  replace --id=... --amp=... --phase=... [--meta=...]
                  query --amp=... --phase=... [--topK=10]
                  queryDetailed --amp=... --phase=... [--topK=10]
                  interference --amp=... --phase=... [--topK=10]
                  interferenceMap --amp=... --phase=... [--topK=10]
                  composite --patterns="amp|phase; amp|phase" [--weights=...] [--topK=10]
                  compositeDetailed --patterns="..." [--weights=...] [--topK=10]
                  exit
                """);
    }

    private static Path resolveDbRoot(Map<String, String> flags) {
        String dbRootStr = firstNonBlank(
                flags.get("db"),
                System.getenv("RESONANCE_DB_ROOT"),
                "./resonancedb-data"
        );
        return Path.of(dbRootStr).toAbsolutePath().normalize();
    }

    private static Map<String, String> parseFlags(String[] args) {
        Map<String, String> out = new HashMap<>();
        if (args == null) return out;

        for (String s : args) {
            if (s == null) continue;
            String a = s.trim();
            if (a.isEmpty()) continue;
            if (!a.startsWith("--")) continue;

            a = a.substring(2);
            int eq = a.indexOf('=');
            if (eq > 0) {
                out.put(a.substring(0, eq).trim().toLowerCase(Locale.ROOT), a.substring(eq + 1).trim());
            } else {
                out.put(a.trim().toLowerCase(Locale.ROOT), "true");
            }
        }
        return out;
    }

    private static List<String> parsePositional(String[] args) {
        List<String> out = new ArrayList<>();
        if (args == null) return out;

        for (String s : args) {
            if (s == null) continue;
            String a = s.trim();
            if (a.isEmpty()) continue;
            if (a.startsWith("--")) continue;
            out.add(a);
        }
        return out;
    }

    private static String require(Map<String, String> flags, String key) {
        String v = flags.get(key.toLowerCase(Locale.ROOT));
        if (v == null || v.trim().isEmpty()) throw new IllegalArgumentException("Missing --" + key);
        return v.trim();
    }

    private static String firstNonBlank(String... xs) {
        if (xs == null) return null;
        for (String x : xs) {
            if (x != null) {
                String t = x.trim();
                if (!t.isEmpty()) return t;
            }
        }
        return null;
    }

    private static int parseIntOr(Map<String, String> flags, String key, int def) {
        String v = flags.get(key.toLowerCase(Locale.ROOT));
        if (v == null || v.trim().isEmpty()) return def;
        try {
            return Integer.parseInt(v.trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Bad --" + key + ": " + v);
        }
    }

    private static boolean isTrue(String v) {
        if (v == null) return false;
        String t = v.trim().toLowerCase(Locale.ROOT);
        return "true".equals(t) || "1".equals(t) || "yes".equals(t) || "y".equals(t);
    }

    private static String safeMsg(Throwable t) {
        String m = (t == null) ? null : t.getMessage();
        if (m == null || m.isBlank()) return (t == null) ? "Error" : t.getClass().getSimpleName();
        return m;
    }

    private static String[] splitArgs(String line) {
        List<String> out = new ArrayList<>();
        StringBuilder cur = new StringBuilder();
        boolean q = false;

        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);

            if (c == '"') {
                q = !q;
                continue;
            }
            if (!q && Character.isWhitespace(c)) {
                if (cur.length() > 0) {
                    out.add(cur.toString());
                    cur.setLength(0);
                }
            } else {
                cur.append(c);
            }
        }

        if (cur.length() > 0) out.add(cur.toString());
        return out.toArray(new String[0]);
    }

    private record CompositeInput(List<WavePattern> patterns, List<Double> weights) {}
}