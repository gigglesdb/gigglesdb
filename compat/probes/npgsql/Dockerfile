FROM mcr.microsoft.com/dotnet/sdk:9.0.203 AS build
WORKDIR /src
COPY Probe.csproj ./
RUN dotnet restore
COPY Program.cs ./
RUN dotnet publish -c Release --no-restore -o /app
FROM mcr.microsoft.com/dotnet/runtime:9.0.4
WORKDIR /app
COPY --from=build /app ./
ENTRYPOINT ["dotnet", "/app/Probe.dll"]
