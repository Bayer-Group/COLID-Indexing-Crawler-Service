using System;
using System.Collections.Generic;
using COLID.Cache;
using COLID.Cache.Configuration;
using COLID.Graph;
using COLID.Graph.TripleStore.DataModels.Serializers;
using COLID.IndexingCrawlerService.Repositories.Implementation;
using COLID.IndexingCrawlerService.Repositories.Interface;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;

namespace COLID.IndexingCrawlerService.Repositories
{
    public static class RepositoriesModule
    {
        /// <summary>
        /// This will register all the supported functionality by Repositories module.
        /// </summary>
        /// <param name="services">The <see cref="IServiceCollection"/> object for registration.</param>
        /// <param name="configuration">The <see cref="IConfiguration"/> object for registration.</param>
        public static IServiceCollection RegisterRepositoriesModule(this IServiceCollection services, IConfiguration configuration)
        {
            services.AddBaseRepositoriesModule(configuration);
            services.AddSingletonGraphModule(configuration);

            var serializerSettings = new CachingJsonSerializerSettings
            {
                Converters = new List<JsonConverter>(),
                ContractResolver = new DefaultContractResolver
                {
                    NamingStrategy = new CamelCaseNamingStrategy()
                },
                Formatting = Formatting.Indented
            };

            serializerSettings.Converters.Add(new EntityPropertyConverter());

            services.AddDistributedCacheModule(configuration, serializerSettings);

            return services;
        }

        /// <summary>
        /// This will register all the supported functionality by Repositories module for debugging environments.
        /// </summary>
        /// <param name="services">The <see cref="IServiceCollection"/> object for registration.</param>
        /// <param name="configuration">The <see cref="IConfiguration"/> object for registration.</param>
        public static IServiceCollection RegisterDebugRepositoriesModule(this IServiceCollection services, IConfiguration configuration)
        {
            services.AddBaseRepositoriesModule(configuration);
            services.AddSingletonGraphModule(configuration);

            var serializerSettings = new CachingJsonSerializerSettings
            {
                Converters = new List<JsonConverter>(),
                ContractResolver = new DefaultContractResolver
                {
                    NamingStrategy = new CamelCaseNamingStrategy()
                },
                Formatting = Formatting.Indented
            };

            serializerSettings.Converters.Add(new EntityPropertyConverter());

            services.AddMemoryCacheModule(configuration, serializerSettings);

            return services;
        }

        /// <summary>
        /// This will register all the supported functionality by Repositories module for local environments.
        /// </summary>
        /// <param name="services">The <see cref="IServiceCollection"/> object for registration.</param>
        /// <param name="configuration">The <see cref="IConfiguration"/> object for registration.</param>
        public static IServiceCollection RegisterLocalRepositoriesModule(this IServiceCollection services, IConfiguration configuration)
        {
            services.AddBaseRepositoriesModule(configuration);
            services.AddSingletonGraphModule(configuration);

            var serializerSettings = new CachingJsonSerializerSettings
            {
                Converters = new List<JsonConverter>(),
                ContractResolver = new DefaultContractResolver
                {
                    NamingStrategy = new CamelCaseNamingStrategy()
                },
                Formatting = Formatting.Indented
            };

            serializerSettings.Converters.Add(new EntityPropertyConverter());

            services.AddMemoryCacheModule(configuration, serializerSettings);

            return services;
        }

        /// <summary>
        /// This will register all the supported functionality by Repositories module for debugging environments.
        /// </summary>
        /// <param name="services">The <see cref="IServiceCollection"/> object for registration.</param>
        /// <param name="configuration">The <see cref="IConfiguration"/> object for registration.</param>
        private static IServiceCollection AddBaseRepositoriesModule(this IServiceCollection services, IConfiguration configuration)
        {
            services.AddTransient<IResourceRepository, ResourceRepository>();
            services.AddTransient<IEntityRepository, EntityRepository>();

            return services;
        }
    }
}
