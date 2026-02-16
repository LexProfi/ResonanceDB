# ---- build stage ----
FROM eclipse-temurin:22-jdk AS build
WORKDIR /src
COPY . .

# Важно: check включает генерацию THIRD_PARTY_NOTICES + запрет GPL
# resonance-server:assembleServer собирает единый дистрибутив (server + cli + licenses)
RUN ./gradlew --no-daemon clean check :resonance-server:assembleServer

# ---- runtime stage ----
FROM eclipse-temurin:22-jre
WORKDIR /app

RUN useradd -r -u 10001 appuser

# runtime bundle
COPY --from=build /src/resonance-server/build/server/ /app/

RUN chown -R appuser:appuser /app
USER appuser

ENV PORT=8080
EXPOSE 8080

# если приложению нужны preview/native-access — прокидываем без правок Gradle
ENV JAVA_TOOL_OPTIONS="--enable-preview --enable-native-access=ALL-UNNAMED"

# стартуем REST-сервер (CLI лежит рядом: /app/cli/bin/resonance-cli)
CMD ["/app/server/bin/resonance-rest"]