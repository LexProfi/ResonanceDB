/*
 * ResonanceDB — Waveform Semantic Engine
 * Copyright © 2025-2026 Aleksandr Listopad
 * SPDX-License-Identifier: LicenseRef-ResonanceDB-License-v1.0
 *
 * Patent notice: The authors intend to seek patent protection for this software.
 * Commercial use >30 days → license@evacortex.ai
 */
package ai.evacortex.resonancedb.rest;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

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

import static org.junit.jupiter.api.Assertions.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public final class ResonanceDBRestEndpointIT {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Duration TIMEOUT = Duration.ofSeconds(10);

    private HttpClient http;
    private ResonanceDBRest rest;
    private Path dbRoot;
    private int port;

    @BeforeAll
    void startServer() throws Exception {
        this.http = HttpClient.newBuilder()
                .connectTimeout(TIMEOUT)
                .build();

        this.dbRoot = Files.createTempDirectory("resonancedb-rest-it-");
        this.port = findFreePort();

        this.rest = ResonanceDBRest.withEmbeddedStore(dbRoot, port);
        this.rest.start();
    }

    @AfterAll
    void stopServer() throws Exception {
        if (rest != null) rest.close();
        deleteRecursively(dbRoot);
    }


    @Test
    void health_ok() throws Exception {
        HttpResponse<String> r = get("/health");
        assertEquals(200, r.statusCode());
        assertHeaderContains(r, "content-type", "application/json");
        assertHeaderContains(r, "cache-control", "no-store");
        assertEquals("*", header(r, "access-control-allow-origin"));

        JsonNode json = parse(r.body());
        assertEquals("ok", json.get("status").asText());
        assertTrue(json.get("timeUtc").asText().endsWith("Z"), "timeUtc must be Instant ISO-8601 with Z");
    }

    @Test
    void insert_query_replace_delete_happyPath() throws Exception {
        HttpResponse<String> ins = postJson("/insert",
                "{\\\"pattern\\\":{\\\"amplitude\\\":[1,0.5,0.1],\\\"phase\\\":[0,0.1,0.2]},\\\"metadata\\\":{\\\"name\\\":\\\"test\\\"}}");
        assertEquals(200, ins.statusCode());
        String id = parse(ins.body()).get("id").asText();
        assertTrue(id.matches("^[0-9a-fA-F]{32}$"), "id must be 32-hex MD5");

        HttpResponse<String> q = postJson("/query",
                "{\\\"query\\\":{\\\"amplitude\\\":[1,0.5,0.1],\\\"phase\\\":[0,0.1,0.2]},\\\"topK\\\":5}");
        assertEquals(200, q.statusCode());
        JsonNode qArr = parse(q.body());
        assertTrue(qArr.isArray());

        HttpResponse<String> rep = postJson("/replace",
                "{\\\"id\\\":\\\"" + id + "\\\",\\\"pattern\\\":{\\\"amplitude\\\":[2,1,0.5],\\\"phase\\\":[0,0.2,0.4]},\\\"metadata\\\":{\\\"name\\\":\\\"updated\\\"}}");
        assertEquals(200, rep.statusCode());
        assertEquals(id, parse(rep.body()).get("id").asText(), "replace should keep the same id key");

        HttpResponse<String> del = postJson("/delete",
                "{\\\"id\\\":\\\"" + id + "\\\"}");
        assertEquals(200, del.statusCode());
        assertTrue(parse(del.body()).get("ok").asBoolean());
    }

    @Test
    void insert_duplicate_returns_409_duplicate() throws Exception {
        HttpResponse<String> ins1 = postJson("/insert",
                "{\\\"pattern\\\":{\\\"amplitude\\\":[9,8,7],\\\"phase\\\":[0,0.1,0.2]},\\\"metadata\\\":{}}");
        assertEquals(200, ins1.statusCode());
        String id = parse(ins1.body()).get("id").asText();

        HttpResponse<String> ins2 = postJson("/insert",
                "{\\\"pattern\\\":{\\\"amplitude\\\":[9,8,7],\\\"phase\\\":[0,0.1,0.2]},\\\"metadata\\\":{}}");
        assertEquals(409, ins2.statusCode());
        JsonNode err = parse(ins2.body());
        assertEquals("duplicate", err.get("code").asText());
        assertTrue(err.get("message").asText().contains(id));
    }

    @Test
    void bad_json_returns_400_bad_json() throws Exception {
        HttpResponse<String> r = postRaw("/query", "{bad json}");
        assertEquals(400, r.statusCode());
        JsonNode err = parse(r.body());
        assertEquals("bad_json", err.get("code").asText());
    }

    @Test
    void empty_body_returns_400_bad_request() throws Exception {
        HttpResponse<String> r = postRaw("/query", "");
        assertEquals(400, r.statusCode());
        JsonNode err = parse(r.body());
        assertEquals("bad_request", err.get("code").asText());
        assertTrue(err.get("message").asText().toLowerCase().contains("empty"));
    }

    @Test
    void cors_preflight_options_returns_204() throws Exception {
        HttpRequest req = HttpRequest.newBuilder(uri("/query"))
                .method("OPTIONS", HttpRequest.BodyPublishers.noBody())
                .timeout(TIMEOUT)
                .build();

        HttpResponse<String> r = http.send(req, HttpResponse.BodyHandlers.ofString());
        assertEquals(204, r.statusCode());
        assertEquals("*", header(r, "access-control-allow-origin"));
        assertNotNull(header(r, "access-control-allow-methods"));
        assertNotNull(header(r, "access-control-allow-headers"));
    }

    private HttpResponse<String> get(String path) throws Exception {
        HttpRequest req = HttpRequest.newBuilder(uri(path))
                .GET()
                .timeout(TIMEOUT)
                .build();
        return http.send(req, HttpResponse.BodyHandlers.ofString());
    }

    private HttpResponse<String> postJson(String path, String escapedJson) throws Exception {
        String json = escapedJson.replace("\\\"", "\"");
        return postRaw(path, json);
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

    private static JsonNode parse(String body) throws IOException {
        return MAPPER.readTree(body);
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
        if (root == null) return;
        if (!Files.exists(root)) return;

        Files.walkFileTree(root, new SimpleFileVisitor<>() {

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                try {
                    Files.deleteIfExists(file);
                } catch (IOException e) {
                    try {
                        file.toFile().setWritable(true);
                        Files.deleteIfExists(file);
                    } catch (IOException ex) {
                        throw ex;
                    }
                }
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                if (exc != null) {
                    throw exc;
                }
                try {
                    Files.deleteIfExists(dir);
                } catch (IOException e) {
                    try {
                        dir.toFile().setWritable(true);
                        Files.deleteIfExists(dir);
                    } catch (IOException ex) {
                        throw ex;
                    }
                }
                return FileVisitResult.CONTINUE;
            }
        });
    }
}