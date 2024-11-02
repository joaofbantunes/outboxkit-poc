FROM mcr.microsoft.com/dotnet/aspnet:8.0-jammy-chiseled AS base
WORKDIR /app
EXPOSE 8080
EXPOSE 443

FROM mcr.microsoft.com/dotnet/sdk:8.0-jammy AS build
WORKDIR /code
COPY ["samples/MySqlEndToEndPollingSample/Producer/Producer.csproj", "samples/MySqlEndToEndPollingSample/Producer/"]
COPY ["src/Core/Core.csproj", "src/Core/"]
COPY ["src/MySql/MySql.csproj", "src/MySql/"]
COPY ["Directory.Build.props", "."]
COPY ["Directory.Packages.props", "."]
COPY ["global.json", "."]
RUN dotnet restore "samples/MySqlEndToEndPollingSample/Producer/Producer.csproj"
COPY . .
WORKDIR "/code/samples/MySqlEndToEndPollingSample/Producer"
RUN dotnet build "Producer.csproj" -c Release -o /app/build

FROM build AS publish
RUN dotnet publish "Producer.csproj" -c Release -o /app/publish

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "Producer.dll"]