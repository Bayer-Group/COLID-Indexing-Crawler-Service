using COLID.IndexingCrawlerService.Repositories;
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
        public void ConfigureDockerServices(IServiceCollection services)
        {
            ConfigureServices(services);
            services.RegisterRepositoriesModule(Configuration);
        }
    }
}
