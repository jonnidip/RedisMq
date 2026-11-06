# syntax=docker/dockerfile:1
FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src

COPY Directory.Build.props ./
COPY src/RedisMQ.Broker/RedisMQ.Broker.csproj src/RedisMQ.Broker/
COPY src/RedisMQ.Redis/RedisMQ.Redis.csproj src/RedisMQ.Redis/
COPY src/RedisMQ.Hosting/RedisMQ.Hosting.csproj src/RedisMQ.Hosting/
COPY samples/RedisMQ.Host/RedisMQ.Host.csproj samples/RedisMQ.Host/
RUN dotnet restore samples/RedisMQ.Host/RedisMQ.Host.csproj

COPY . .
RUN dotnet publish samples/RedisMQ.Host/RedisMQ.Host.csproj -c Release -o /app --no-restore

FROM mcr.microsoft.com/dotnet/aspnet:10.0 AS runtime
WORKDIR /app
COPY --from=build /app .

ENV ASPNETCORE_URLS=http://+:8080
EXPOSE 5672 5671 8080
USER $APP_UID
ENTRYPOINT ["dotnet", "RedisMQ.Host.dll"]
