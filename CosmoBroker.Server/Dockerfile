# Build stage
FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src

# Copy all csproj files first for layer-cached restore
# (CosmoBroker.Server references the core CosmoBroker project)
COPY CosmoBroker.Server/CosmoBroker.Server.csproj CosmoBroker.Server/
COPY CosmoBroker.csproj ./
RUN dotnet restore CosmoBroker.Server/CosmoBroker.Server.csproj

# Copy full source and publish (--no-restore: restore was already done above)
COPY . .
RUN dotnet publish CosmoBroker.Server/CosmoBroker.Server.csproj -c Release --no-restore -o /app/publish

# Runtime stage — use runtime-deps (smaller) since we publish a self-contained app
FROM mcr.microsoft.com/dotnet/runtime:10.0
WORKDIR /app

# Run as non-root user
RUN useradd --no-create-home --shell /bin/false cosmobroker
USER cosmobroker

COPY --from=build /app/publish .

# Broker port + monitoring port
EXPOSE 4222
EXPOSE 8222

# Invariant globalization: no ICU dependency, faster startup, smaller image.
ENV DOTNET_SYSTEM_GLOBALIZATION_INVARIANT=true

# Inline socket I/O completions on the epoll/IOCP thread instead of posting to the
# thread pool. Eliminates a thread-pool round-trip per received message — the single
# biggest latency/throughput lever for a high-frequency socket application.
ENV DOTNET_SYSTEM_NET_SOCKETS_INLINE_COMPLETIONS=1

# Server GC for high-throughput workloads (default in server processes, but explicit).
ENV DOTNET_GCConserveMemory=0

# Ensure Docker stop sends SIGTERM so graceful lame-duck shutdown fires.
STOPSIGNAL SIGTERM

# Lightweight healthcheck: TCP connect to the broker port.
HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
  CMD sh -c 'echo "" | nc -w 1 localhost 4222 && exit 0 || exit 1'

ENTRYPOINT ["dotnet", "CosmoBroker.Server.dll"]
