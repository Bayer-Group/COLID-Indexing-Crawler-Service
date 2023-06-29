#See https://aka.ms/containerfastmode to understand how Visual Studio uses this Dockerfile to build your images for faster debugging.
FROM mcr.microsoft.com/dotnet/core/sdk:3.1 AS build
WORKDIR /src

# copy csproj and restore as distinct layers
COPY . .
RUN dotnet restore COLID.IndexingCrawlerService.WebApi/COLID.IndexingCrawlerService.WebApi.csproj 

# build app
WORKDIR /src/COLID.IndexingCrawlerService.WebApi
RUN dotnet publish -c release -o /app --no-restore

# final stage/image
FROM mcr.microsoft.com/dotnet/core/aspnet:3.1-alpine

ARG BUILD_CIPIPELINEID
ENV Build__CiPipelineId=${BUILD_CIPIPELINEID}

ARG BUILD_CICOMMITSHA
ENV Build__CiCommitSha=${BUILD_CICOMMITSHA}

ENV PORT=8080
ENV ASPNETCORE_URLS=http://*:${PORT}
EXPOSE $PORT
WORKDIR /app
COPY --from=build /app ./
CMD ["dotnet", "COLID.IndexingCrawlerService.WebApi.dll"]
