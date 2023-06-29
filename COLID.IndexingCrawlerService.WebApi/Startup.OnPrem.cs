using System;
using COLID.IndexingCrawlerService.Repositories;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace COLID.IndexingCrawlerService.WebApi
{
    /// <summary>
    /// The class to handle statup operations.
    /// </summary>
    public partial class Startup
    {
        /// <summary>
        /// This method gets called by the runtime. Use this method to add services to the container.
        /// </summary>
        /// <param name="services">The <see cref="IServiceCollection"/>The service collection</param>
        public void ConfigureOnPremServices(IServiceCollection services)
        {
            var builder = new ConfigurationBuilder()
                .AddConfiguration(Configuration)
                .AddUserSecrets<Startup>();

            Configuration = builder.Build();

            ConfigureServices(services);
            services.RegisterDebugRepositoriesModule(Configuration);
        }

        /// <summary>
        /// This method gets called by the runtime. Use this method to configure the HTTP request pipeline.
        /// </summary>
        /// <param name="app">The application builder</param>        
        public void ConfigureOnPrem(IApplicationBuilder app)
        {
            app.UseDeveloperExceptionPage();

            // Following problem still exists with .NET Core 3.1 (latest test on 2020-04-27):
            // Accessing TLS secured APIs with the new SocketsHttpHandler introduced in .NET Core 2.1 results into exception "No such host is known".
            // Setting the flag "System.Net.Http.UseSocketsHttpHandler" to false deactivates the usage of the new SocketsHttpHandler and activates the older HttpClientHandler class instead.
            // See https://docs.microsoft.com/en-us/dotnet/api/system.net.http.socketshttphandler?view=netcore-3.1#remarks
            AppContext.SetSwitch("System.Net.Http.UseSocketsHttpHandler", false);

            Configure(app);
        }
    }
}
