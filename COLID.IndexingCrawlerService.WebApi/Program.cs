using System.Net.Http;
using COLID.Graph;
using COLID.IndexingCrawlerService.Repositories;
using COLID.IndexingCrawlerService.Services;
using COLID.MessageQueue;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

var builder = WebApplication.CreateBuilder(args);
ConfigurationManager configuration = builder.Configuration;

builder.Services.AddCors();
builder.Services.AddHttpContextAccessor();
builder.Services.AddHttpClient("NoProxy").ConfigurePrimaryHttpMessageHandler(() =>
{
    return new HttpClientHandler
    {
        UseProxy = false,
        Proxy = null
    };
});
builder.Services.AddHealthChecks();
builder.Services.AddControllers();
builder.Services.AddHashGeneratorModule();
builder.Services.AddSingleton(configuration);
builder.Services.AddServicesModule(configuration);
builder.Services.AddMessageQueueModule(configuration);

if (builder.Environment.IsEnvironment("Local"))
{
    builder.Services.RegisterLocalRepositoriesModule(configuration);
}
else
{
    builder.Services.RegisterRepositoriesModule(configuration);
}

var app = builder.Build();

if (builder.Environment.IsEnvironment("Local") || builder.Environment.IsEnvironment("Development"))
{
    app.UseDeveloperExceptionPage();
}

app.UseHttpsRedirection();

app.UseRouting();

app.UseCors(
    options => options.SetIsOriginAllowed(x => _ = true)
    .AllowAnyMethod()
    .AllowAnyHeader()
    .AllowCredentials()
);

app.UseAuthentication();
app.UseAuthorization();

app.UseEndpoints(endpoints =>
{
    endpoints.MapHealthChecks("/health");
    endpoints.MapControllers();
});

app.UseMessageQueueModule(configuration);

app.Run();
