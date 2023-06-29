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
        public void ConfigureLocalServices(IServiceCollection services)
        {
            var builder = new ConfigurationBuilder()
                .AddConfiguration(Configuration)
                .AddUserSecrets<Startup>();

            Configuration = builder.Build();

            ConfigureServices(services);

            services.RegisterLocalRepositoriesModule(Configuration);
        }

        /// <summary>
        /// This method gets called by the runtime. Use this method to configure the HTTP request pipeline.
        /// </summary>
        /// <param name="app">The application builder</param>        
        public void ConfigureLocal(IApplicationBuilder app)
        {
            app.UseDeveloperExceptionPage();
            Configure(app);
        }
    }
}
