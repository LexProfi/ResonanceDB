/*
 * ResonanceDB — Waveform Semantic Engine
 * Copyright © 2025-2026 Aleksandr Listopad
 * SPDX-License-Identifier: LicenseRef-ResonanceDB-License-v1.0
 *
 * Patent notice: The authors intend to seek patent protection for this software.
 * Commercial use >30 days → license@evacortex.ai
 */
package ai.evacortex.resonancedb.rest;

import ai.evacortex.resonancedb.rest.dto.*;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Duration;
import java.util.Arrays;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public final class ResonanceDBRestEndpointIT {

    private static final Duration TIMEOUT = Duration.ofSeconds(10);
    private static final ObjectMapper MAPPER = new ObjectMapper().findAndRegisterModules();
    private static final String PROP_PATTERN_LEN = "resonance.pattern.len";
    private static final String CORPUS_ID = "it-default";

    private HttpClient http;
    private ResonanceDBRest rest;
    private int port;
    private Path dbRoot;

    private static int patternLen() {
        return Integer.getInteger(PROP_PATTERN_LEN, 1536);
    }

    @BeforeAll
    void startServer() throws Exception {
        this.http = HttpClient.newBuilder()
                .connectTimeout(TIMEOUT)
                .build();

        this.port = findFreePort();
        this.dbRoot = Files.createTempDirectory("resonancedb-rest-it-");

        this.rest = ResonanceDBRest.withEmbeddedStore(dbRoot, port);
        assertNotNull(rest, "ResonanceDBRest.withEmbeddedStore(...) returned null");
        this.rest.start();

        HttpResponse<String> r = get("/health");
        assertEquals(200, r.statusCode(), "server didn't start properly: " + r.body());
    }

    @AfterAll
    void stopServer() throws Exception {
        if (rest != null) {
            try {
                rest.close();
            } finally {
                rest = null;
            }
        }
        deleteRecursively(dbRoot);
        dbRoot = null;
    }

    @Test
    void health_ok() throws Exception {
        HttpResponse<String> r = get("/health");

        assertEquals(200, r.statusCode(), r.body());
        assertHeaderContains(r, "content-type", "application/json");
        assertHeaderContains(r, "cache-control", "no-store");
        assertEquals("*", header(r, "access-control-allow-origin"));

        HealthResponse json = read(r, HealthResponse.class);
        assertEquals("ok", json.status());
        assertNotNull(json.timeUtc());
        assertTrue(json.timeUtc().toString().endsWith("Z"), "timeUtc must be Instant ISO-8601 with Z");
    }

    @Test
    void insert_query_replace_delete_happyPath() throws Exception {
        int len = patternLen();

        WavePatternDto p1 = constantDto(1.0, 0.1, len);
        HttpResponse<String> ins = post(corpusPath("/insert"), new InsertRequest(p1, Map.of("name", "test")));
        assertEquals(200, ins.statusCode(), ins.body());

        String oldId = read(ins, IdResponse.class).id();
        assertTrue(oldId.matches("^[0-9a-fA-F]{32}$"), "id must be 32-hex MD5");

        HttpResponse<String> q1 = post(corpusPath("/query"), new QueryRequest(p1, 5));
        assertEquals(200, q1.statusCode(), q1.body());
        assertQueryArrayContainsId(q1.body(), oldId, "query(inserted) must contain inserted id");

        WavePatternDto p2 = constantDto(2.0, 0.2, len);
        HttpResponse<String> rep = post(corpusPath("/replace"), new ReplaceRequest(oldId, p2, Map.of("name", "updated")));
        assertEquals(200, rep.statusCode(), rep.body());

        String newId = read(rep, IdResponse.class).id();
        assertNotNull(newId);
        assertTrue(newId.matches("^[0-9a-fA-F]{32}$"), "id must be 32-hex MD5");
        assertNotEquals(oldId, newId, "replace must change id because id is a content hash");

        HttpResponse<String> q2 = post(corpusPath("/query"), new QueryRequest(p2, 5));
        assertEquals(200, q2.statusCode(), q2.body());
        assertQueryArrayContainsId(q2.body(), newId, "query(updated) must contain replaced id");

        HttpResponse<String> delOld = post(corpusPath("/delete"), new DeleteRequest(oldId));
        assertTrue(
                delOld.statusCode() == 404 || delOld.statusCode() == 409,
                "deleting old id must fail (expected 404/409), got: " + delOld.statusCode() + " body=" + delOld.body()
        );

        HttpResponse<String> delNew = post(corpusPath("/delete"), new DeleteRequest(newId));
        assertEquals(200, delNew.statusCode(), delNew.body());
        assertTrue(read(delNew, OkResponse.class).ok());
    }

    @Test
    void insert_duplicate_returns_409_duplicate() throws Exception {
        int len = patternLen();
        WavePatternDto p = constantDto(9.0, 0.3, len);

        HttpResponse<String> ins1 = post(corpusPath("/insert"), new InsertRequest(p, Map.of()));
        assertEquals(200, ins1.statusCode(), ins1.body());
        String id = read(ins1, IdResponse.class).id();

        HttpResponse<String> ins2 = post(corpusPath("/insert"), new InsertRequest(p, Map.of()));
        assertEquals(409, ins2.statusCode(), ins2.body());

        ErrorResponse err = read(ins2, ErrorResponse.class);
        assertEquals("duplicate", err.code());
        assertNotNull(err.message());
        assertTrue(err.message().contains(id), "error message must mention existing id");
    }

    @Test
    void bad_json_returns_400_bad_json() throws Exception {
        HttpResponse<String> r = postRaw(corpusPath("/query"), "{bad json}");
        assertEquals(400, r.statusCode(), r.body());

        ErrorResponse err = read(r, ErrorResponse.class);
        assertEquals("bad_json", err.code());
    }

    @Test
    void empty_body_returns_400_bad_request() throws Exception {
        HttpResponse<String> r = postRaw(corpusPath("/query"), "");
        assertEquals(400, r.statusCode(), r.body());

        ErrorResponse err = read(r, ErrorResponse.class);
        assertEquals("bad_request", err.code());
        assertNotNull(err.message());
        assertTrue(err.message().toLowerCase().contains("empty"));
    }

    @Test
    void cors_preflight_options_returns_204() throws Exception {
        HttpRequest req = HttpRequest.newBuilder(uri(corpusPath("/query")))
                .method("OPTIONS", HttpRequest.BodyPublishers.noBody())
                .timeout(TIMEOUT)
                .build();

        HttpResponse<String> r = http.send(req, HttpResponse.BodyHandlers.ofString());
        assertEquals(204, r.statusCode(), r.body());
        assertEquals("*", header(r, "access-control-allow-origin"));
        assertNotNull(header(r, "access-control-allow-methods"));
        assertNotNull(header(r, "access-control-allow-headers"));
    }

    private static WavePatternDto constantDto(double amp, double phase, int len) {
        double[] a = new double[len];
        double[] p = new double[len];
        for (int i = 0; i < len; i++) {
            a[i] = amp;
            p[i] = phase;
        }
        return new WavePatternDto(a, p);
    }

    private String corpusPath(String suffix) {
        return "/corpora/" + CORPUS_ID + suffix;
    }

    private HttpResponse<String> get(String path) throws Exception {
        HttpRequest req = HttpRequest.newBuilder(uri(path))
                .GET()
                .timeout(TIMEOUT)
                .build();
        return http.send(req, HttpResponse.BodyHandlers.ofString());
    }

    private HttpResponse<String> post(String path, Object body) throws Exception {
        return postRaw(path, MAPPER.writeValueAsString(body));
    }

    private HttpResponse<String> postRaw(String path, String body) throws Exception {
        HttpRequest req = HttpRequest.newBuilder(uri(path))
                .timeout(TIMEOUT)
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();
        return http.send(req, HttpResponse.BodyHandlers.ofString());
    }

    private URI uri(String path) {
        return URI.create("http://localhost:" + port + path);
    }

    private static <T> T read(HttpResponse<String> r, Class<T> type) throws IOException {
        return MAPPER.readValue(r.body(), type);
    }

    private static void assertQueryArrayContainsId(String body, String expectedId, String messageIfMissing) throws IOException {
        JsonNode arr = MAPPER.readTree(body);
        assertTrue(arr.isArray(), "query must return JSON array");
        boolean found = false;
        for (JsonNode n : arr) {
            JsonNode idNode = n.get("id");
            if (idNode != null && expectedId.equalsIgnoreCase(idNode.asText())) {
                found = true;
                break;
            }
        }
        assertTrue(found, messageIfMissing + " (expectedId=" + expectedId + ")");
    }

    private static String header(HttpResponse<?> r, String nameLower) {
        return r.headers().firstValue(nameLower).orElse(null);
    }

    private static void assertHeaderContains(HttpResponse<?> r, String nameLower, String contains) {
        String v = header(r, nameLower);
        assertNotNull(v, "missing header: " + nameLower);
        assertTrue(v.toLowerCase().contains(contains.toLowerCase()),
                "header " + nameLower + " must contain '" + contains + "', got: " + v);
    }

    private static int findFreePort() throws IOException {
        try (ServerSocket s = new ServerSocket(0)) {
            s.setReuseAddress(true);
            return s.getLocalPort();
        }
    }

    private static void deleteRecursively(Path root) throws IOException {
        if (root == null || !Files.exists(root)) {
            return;
        }

        Files.walkFileTree(root, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                Files.deleteIfExists(file);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                if (exc != null) {
                    throw exc;
                }
                Files.deleteIfExists(dir);
                return FileVisitResult.CONTINUE;
            }
        });
    }
}