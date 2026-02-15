/*
 * ResonanceDB — Waveform Semantic Engine
 * Copyright © 2025 Aleksandr Listopad
 * SPDX-License-Identifier: LicenseRef-ResonanceDB-License-v1.0
 *
 * Patent notice: The authors intend to seek patent protection for this software.
 * Commercial use >30 days → license@evacortex.ai
 */
package ai.evacortex.resonancedb.store;

import ai.evacortex.resonancedb.core.storage.WavePattern;
import ai.evacortex.resonancedb.core.storage.WavePatternStoreImpl;
import ai.evacortex.resonancedb.core.storage.responce.ResonanceMatch;
import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.function.Function;

import static java.lang.Math.*;
import static org.junit.jupiter.api.Assertions.*;

@TestInstance(TestInstance.Lifecycle.PER_METHOD)
public class RetrievalQualityEvaluationTest {

    private static final String DEFAULT_JSON_PATH = "C:\\ResonanceDB\\wave_patterns.json";
    private static final int TOP_K = Integer.getInteger("resonance.eval.topK", 10);

    private static final String COSINE_MODE =
            System.getProperty("resonance.eval.cosineMode", "complex");

    private static final String OFFSETS_PROP =
            System.getProperty("resonance.eval.phaseOffsets",
                    "0.0,0.3,-0.3,1.5707963267948966,-1.5707963267948966");


    private static final boolean REQUIRE_RESONANCE_WIN =
            Boolean.parseBoolean(System.getProperty("resonance.eval.requireWin", "false"));


    @TempDir
    Path tempDir;

    @JsonIgnoreProperties(ignoreUnknown = true)
    static final class JsonPattern {
        @JsonAlias({"id"})
        public String id;

        @JsonAlias({"label", "text"})
        public String label;

        @JsonAlias({"amplitude", "amp"})
        public double[] amplitude;

        @JsonAlias({"phase"})
        public double[] phase;
    }

    private static List<JsonPattern> loadJson(Path path) throws IOException {
        ObjectMapper om = new ObjectMapper();
        JsonPattern[] arr = om.readValue(Files.readAllBytes(path), JsonPattern[].class);
        return Arrays.asList(arr);
    }

    private static WavePattern toWave(JsonPattern jp) {
        return new WavePattern(jp.amplitude, jp.phase);
    }

    private static double[] complexRe(double[] amp, double[] phase) {
        double[] re = new double[amp.length];
        for (int i = 0; i < amp.length; i++) re[i] = amp[i] * Math.cos(phase[i]);
        return re;
    }

    private static double[] complexIm(double[] amp, double[] phase) {
        double[] im = new double[amp.length];
        for (int i = 0; i < amp.length; i++) im[i] = amp[i] * Math.sin(phase[i]);
        return im;
    }

    private static double[] concat(double[] x, double[] y) {
        double[] z = new double[x.length + y.length];
        System.arraycopy(x, 0, z, 0, x.length);
        System.arraycopy(y, 0, z, x.length, y.length);
        return z;
    }

    private static double cosine(double[] a, double[] b) {
        double dot = 0, na = 0, nb = 0;
        for (int i = 0; i < a.length; i++) {
            dot += a[i] * b[i];
            na += a[i] * a[i];
            nb += b[i] * b[i];
        }
        if (na == 0 || nb == 0) return 0.0;
        return dot / (Math.sqrt(na) * Math.sqrt(nb));
    }

    private static double[] cosineEmbedding(double[] amp, double[] phase) {
        return switch (COSINE_MODE) {
            case "amp" -> amp;
            case "amp+phase" -> concat(amp, phase);
            case "complex" -> {
                double[] re = complexRe(amp, phase);
                double[] im = complexIm(amp, phase);
                yield concat(re, im);
            }
            default -> throw new IllegalArgumentException("Unknown cosine mode: " + COSINE_MODE);
        };
    }

    private static WavePattern shifted(WavePattern src, double delta) {
        double[] amp = src.amplitude().clone();
        double[] ph = src.phase().clone();
        for (int i = 0; i < ph.length; i++) {
            double x = ph[i] + delta;
            if (x > Math.PI) x -= 2 * Math.PI;
            if (x < -Math.PI) x += 2 * Math.PI;
            ph[i] = x;
        }
        return new WavePattern(amp, ph);
    }

    private static String fmt(double v) {
        DecimalFormatSymbols s = DecimalFormatSymbols.getInstance(Locale.US);
        return new DecimalFormat("0.0000", s).format(v);
    }

    public record Hit(String id, double score) {}
    public record ResultRow(String queryKind, String gtId, String resTop1, String cosTop1) {}
    private record Triple(WavePattern query, String gtId, String kind) {}

    @Test
    public void evaluateResonanceVsCosine() throws Exception {
        Path jsonPath = Path.of(System.getProperty("resonance.eval.json", DEFAULT_JSON_PATH));
        List<JsonPattern> json = loadJson(jsonPath);
        if (json.isEmpty()) fail("JSON is empty: " + jsonPath);

        Map<String, WavePattern> catalog = new LinkedHashMap<>();
        Map<String, double[]> cosineCat = new LinkedHashMap<>();
        Map<String, String> labelById = new LinkedHashMap<>();

        try (WavePatternStoreImpl store = new WavePatternStoreImpl(tempDir)) {
            for (JsonPattern p : json) {
                WavePattern wp = toWave(p);
                String storeId = store.insert(wp, Map.of("label", p.label == null ? "" : p.label));
                catalog.put(storeId, wp);
                cosineCat.put(storeId, cosineEmbedding(p.amplitude, p.phase));
                labelById.put(storeId, p.label == null ? "" : p.label);
            }

            List<Double> offsets = Arrays.stream(OFFSETS_PROP.split(","))
                    .map(String::trim).filter(s -> !s.isEmpty())
                    .map(Double::parseDouble).toList();

            List<Triple> queries = new ArrayList<>();
            for (Map.Entry<String, WavePattern> e : catalog.entrySet()) {
                String gtId = e.getKey();
                WavePattern base = e.getValue();
                for (double dPhi : offsets) {
                    WavePattern q = (abs(dPhi) < 1e-12) ? base : shifted(base, dPhi);
                    String kind = abs(dPhi) < 1e-12 ? "self" : "shift" + (dPhi >= 0 ? "+" : "") + fmt(dPhi);
                    queries.add(new Triple(q, gtId, kind));
                }
            }

            int total = queries.size();
            int correctTop1Res = 0, correctTop1Cos = 0;
            int recallAtKRes = 0, recallAtKCos = 0;
            double mrrRes = 0.0, mrrCos = 0.0;

            int winsResOnly = 0, winsCosOnly = 0, ties = 0;
            List<ResultRow> qualitative = new ArrayList<>();

            final int N = catalog.size();
            final int k = Math.min(TOP_K, N);

            for (Triple t : queries) {
                List<ResonanceMatch> resAll = store.query(t.query(), N);
                String resTop1 = resAll.isEmpty() ? "<none>" : resAll.get(0).id();
                int rankRes = rankOf(t.gtId(), resAll, ResonanceMatch::id);

                if (rankRes == 1) correctTop1Res++;
                if (rankRes > 0 && rankRes <= k) recallAtKRes++;
                if (rankRes > 0) mrrRes += 1.0 / rankRes;

                double[] qEmb = cosineEmbedding(t.query().amplitude(), t.query().phase());
                List<Hit> cosAll = rankCosineFull(qEmb, cosineCat);
                String cosTop1 = cosAll.isEmpty() ? "<none>" : cosAll.get(0).id();
                int rankCos = rankOf(t.gtId(), cosAll, Hit::id);

                if (rankCos == 1) correctTop1Cos++;
                if (rankCos > 0 && rankCos <= k) recallAtKCos++;
                if (rankCos > 0) mrrCos += 1.0 / rankCos;

                if (Objects.equals(resTop1, cosTop1)) {
                    ties++;
                } else if (Objects.equals(resTop1, t.gtId())) {
                    winsResOnly++;
                } else if (Objects.equals(cosTop1, t.gtId())) {
                    winsCosOnly++;
                }

                if (qualitative.size() < 16) {
                    qualitative.add(new ResultRow(t.kind(), t.gtId(), resTop1, cosTop1));
                }
            }

            double p1Res = correctTop1Res / (double) total;
            double p1Cos = correctTop1Cos / (double) total;
            double rKRes = recallAtKRes / (double) total;
            double rKCos = recallAtKCos / (double) total;
            double mrrResAvg = mrrRes / total;
            double mrrCosAvg = mrrCos / total;

            DecimalFormatSymbols us = DecimalFormatSymbols.getInstance(Locale.US);
            DecimalFormat df = new DecimalFormat("0.0000", us);

            System.out.println("=== Retrieval Quality Evaluation (" +
                    LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")) + ") ===");
            System.out.printf(Locale.US, "Loaded from JSON: %d | Queries: %d | Top-K=%d%n%n",
                    N, total, k);

            System.out.println("=== Retrieval metrics ===");
            System.out.println("P@1   Resonance: " + df.format(p1Res) + "   |   Cosine: " + df.format(p1Cos));
            System.out.println("R@" + k + "  Resonance: " + df.format(rKRes) + "   |   Cosine: " + df.format(rKCos));
            System.out.println("MRR   Resonance: " + df.format(mrrResAvg) + "   |   Cosine: " + df.format(mrrCosAvg));
            System.out.printf(Locale.US, "Top-1 wins — Resonance: %d   |   Cosine: %d   |   ties: %d%n%n",
                    winsResOnly, winsCosOnly, ties);

            System.out.println("=== Qualitative (sample) ===");
            for (ResultRow row : qualitative) {
                String label = labelById.getOrDefault(row.gtId(), "");
                System.out.printf("%-10s | GT=%s (%s) | R@1=%s | C@1=%s%n",
                        trim(row.queryKind(), 10), row.gtId(), label, row.resTop1(), row.cosTop1());
            }

            assertEquals(N * offsets.size(), total, "Bad query count");
            assertTrue(total > 0, "No queries generated");
            if (REQUIRE_RESONANCE_WIN) {
                assertTrue(winsResOnly >= 1,
                        "Expect at least one query where Resonance Top-1 is correct and Cosine is not");
            }
        }
    }

    private static String trim(String s, int n) {
        if (s == null) return "";
        return s.length() <= n ? s : s.substring(0, n);
    }

    private static <T> int rankOf(String gtId, List<T> hits, Function<T, String> idFn) {
        for (int i = 0; i < hits.size(); i++) {
            if (Objects.equals(idFn.apply(hits.get(i)), gtId)) return i + 1;
        }
        return -1;
    }

    private static List<Hit> rankCosineFull(double[] qEmb, Map<String, double[]> catalog) {
        List<Hit> scored = new ArrayList<>(catalog.size());
        for (Map.Entry<String, double[]> e : catalog.entrySet()) {
            double s = cosine(qEmb, e.getValue());
            scored.add(new Hit(e.getKey(), s));
        }
        scored.sort((a, b) -> Double.compare(b.score, a.score));
        return scored;
    }
}