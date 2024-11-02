FROM mcr.microsoft.com/dotnet/aspnet:8.0-jammy-chiseled AS base
WORKDIR /app
EXPOSE 8080
EXPOSE 443

FROM mcr.microsoft.com/dotnet/sdk:8.0-jammy AS build
WORKDIR /code
COPY ["samples/MySqlEndToEndPollingSample/Consumer/Consumer.csproj", "samples/MySqlEndToEndPollingSample/Consumer/"]
COPY ["Directory.Build.props", "."]
COPY ["Directory.Packages.props", "."]
COPY ["global.json", "."]
RUN dotnet restore "samples/MySqlEndToEndPollingSample/Consumer/Consumer.csproj"
COPY . .
WORKDIR "/code/samples/MySqlEndToEndPollingSample/Consumer"
RUN dotnet build "Consumer.csproj" -c Release -o /app/build

FROM build AS publish
RUN dotnet publish "Consumer.csproj" -c Release -o /app/publish

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "Consumer.dll"]