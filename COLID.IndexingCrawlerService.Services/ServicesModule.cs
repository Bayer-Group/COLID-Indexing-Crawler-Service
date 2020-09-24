using AutoMapper;
using COLID.Graph.Metadata.Services;
using COLID.Graph.TripleStore.MappingProfiles;
using COLID.Identity;
using COLID.IndexingCrawlerService.Services.Configuration;
using COLID.IndexingCrawlerService.Services.Implementation;
using COLID.IndexingCrawlerService.Services.Interface;
using COLID.IndexingCrawlerService.Services.MappingProfiles;
using COLID.MessageQueue.Services;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace COLID.IndexingCrawlerService.Services
{
    public static class ServicesModule
    {
        /// <summary>
        /// This will register all the supported functionality by Repositories module.
        /// </summary>
        /// <param name="services">The <see cref="IServiceCollection"/> object for registration.</param>
        /// <param name="configuration">The <see cref="IConfiguration"/> object for registration.</param>
        public static IServiceCollection AddServicesModule(this IServiceCollection services, IConfiguration configuration)
        {
            services.AddAutoMapper(
                typeof(EntityProfile),
                typeof(MetadataPropertyProfile),
                typeof(ResourceProfile));

            services.Configure<ColidRegistrationServiceTokenOptions>(configuration.GetSection("ColidRegistrationServiceTokenOptions"));
            services.Configure<ColidSearchServiceTokenOptions>(configuration.GetSection("ColidSearchServiceTokenOptions"));
            services.AddIdentityModule(configuration);

            services.AddSingleton<IndexingService>();
            services.AddSingleton<IIndexingService>(x => x.GetRequiredService<IndexingService>());
            services.AddSingleton<IMessageQueuePublisher>(x => x.GetRequiredService<IndexingService>());
            services.AddSingleton<IMessageQueueReceiver>(x => x.GetRequiredService<IndexingService>());

            services.AddTransient<IValidationService, ValidationService>();
            services.AddTransient<IEntityService, EntityService>();
            services.AddTransient<IResourceService, ResourceService>();

            return services;
        }
    }
}
