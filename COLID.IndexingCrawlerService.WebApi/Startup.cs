using System.Net.Http;
using COLID.Common.Logger;
using COLID.Graph;
using COLID.IndexingCrawlerService.Services;
using COLID.MessageQueue;
using CorrelationId;
using CorrelationId.DependencyInjection;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace COLID.IndexingCrawlerService.WebApi
{
    public partial class Startup
    {
        /// <summary>
        /// The class to handle startup operations.
        /// </summary>
        /// <param name="configuration">The configuration</param>
        /// <param name="env">The environment</param>
        public Startup(IConfiguration configuration, IWebHostEnvironment env)
        {
            Configuration = configuration;
        }

        /// <summary>
        /// Represents a set of key/value application configuration properties.
        /// </summary>
        public IConfiguration Configuration { get; private set; }

        /// <summary>
        /// This method gets called by the runtime. Use this method to add services to the container.
        /// </summary>
        /// <param name="services"></param>
        public void ConfigureServices(IServiceCollection services)
        {
            services.AddDefaultCorrelationId();
            services.AddCorrelationIdLogger();
            services.AddCors();
            services.AddHttpContextAccessor();
            services.AddHttpClient("NoProxy").ConfigurePrimaryHttpMessageHandler(() =>
            {
                return new HttpClientHandler
                {
                    UseProxy = false,
                    Proxy = null
                };
            });
            services.AddHealthChecks();
            
            services.AddControllers();

            services.AddHashGeneratorModule();

            services.AddSingleton(Configuration);
            services.AddServicesModule(Configuration);
            services.AddMessageQueueModule(Configuration);
        }

        /// <summary>
        /// This method gets called by the runtime. Use this method to configure the HTTP request pipeline.
        /// </summary>
        /// <param name="app"></param>       
        public void Configure(IApplicationBuilder app)
        {
            app.UseCorrelationId();
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

            app.UseMessageQueueModule(Configuration);
        }
    }
}
