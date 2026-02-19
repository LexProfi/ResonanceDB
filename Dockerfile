FROM eclipse-temurin:22-jdk AS build
WORKDIR /src

COPY gradlew settings.gradle* build.gradle* gradle.properties* ./
COPY gradle/ ./gradle/

RUN set -eux; \
    test -f ./gradlew; \
    test -f ./gradle/wrapper/gradle-wrapper.properties; \
    test -f ./gradle/wrapper/gradle-wrapper.jar

COPY . .

RUN ./gradlew --no-daemon clean check :resonance-server:assembleServer

RUN set -eux; \
    test -f resonance-server/build/server/server/bin/resonance-server; \
    test -f resonance-server/build/server/server/bin/resonance-server.bat; \
    test -d resonance-server/build/server/cli/bin; \
    test -f resonance-server/build/server/licenses/LICENSE; \
    test -f resonance-server/build/server/licenses/TRAINING_NOTICE.md; \
    test -f resonance-server/build/server/licenses/THIRD_PARTY_NOTICES.md; \
    test -f resonance-server/build/server/server/VERSION || true

FROM eclipse-temurin:22-jre
WORKDIR /app

RUN set -eux; \
    groupadd -r -g 10001 appuser; \
    useradd  -r -u 10001 -g 10001 -d /nonexistent -s /usr/sbin/nologin appuser; \
    mkdir -p /data; \
    chown -R appuser:appuser /data

COPY --from=build /src/resonance-server/build/server/ /app/

RUN set -eux; \
    chmod +x /app/server/bin/* /app/cli/bin/*; \
    sed -i 's/\r$//' /app/server/bin/* /app/cli/bin/* || true; \
    chown -R appuser:appuser /app

USER appuser

ENV PORT=31415
ENV RESONANCE_DB_ROOT=/data

EXPOSE 31415

ENV JAVA_TOOL_OPTIONS="-XX:+UseG1GC -XX:G1HeapRegionSize=8m -XX:MaxGCPauseMillis=100 -Xms512m -Xmx512m -Dresonance.kernel.native=false"

HEALTHCHECK --interval=10s --timeout=2s --start-period=10s --retries=6 \
  CMD /app/cli/bin/resonance-cli health --url "http://127.0.0.1:${PORT}/health" || exit 1

ENTRYPOINT ["/app/server/bin/resonance-server"]
CMD ["--db=/data", "--port=31415"]