# Build stage
FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src

# Copy csproj and restore
COPY CosmoBroker.Server/CosmoBroker.Server.csproj CosmoBroker.Server/
RUN dotnet restore CosmoBroker.Server/CosmoBroker.Server.csproj

# Copy source
COPY . .
RUN dotnet publish CosmoBroker.Server/CosmoBroker.Server.csproj -c Release -o /app/publish

# Runtime stage
FROM mcr.microsoft.com/dotnet/runtime:10.0
WORKDIR /app
COPY --from=build /app/publish .

EXPOSE 4222

ENV DOTNET_SYSTEM_GLOBALIZATION_INVARIANT=false

ENTRYPOINT ["dotnet", "CosmoBroker.Server.dll"]
