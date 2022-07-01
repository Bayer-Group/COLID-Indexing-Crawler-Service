using System;
using System.Collections.Generic;
using System.Linq;
using COLID.IndexingCrawlerService.Repositories.Interface;
using COLID.IndexingCrawlerService.Services.Interface;
using COLID.Graph.Metadata.DataModels.Resources;
using COLID.Graph.Metadata.Services;
using COLID.RegistrationService.Common.DataModel.Resources;
using COLID.Cache.Services;
using Microsoft.Extensions.Logging;

namespace COLID.IndexingCrawlerService.Services.Implementation
{
    public class ResourceService : IResourceService
    {
        private readonly IResourceRepository _resourceRepository;
        private readonly IMetadataService _metadataService;
        private readonly ICacheService _cacheService;
        private readonly ILogger<ResourceService> _logger;

        public ResourceService(IResourceRepository resourceRepository, IMetadataService metadataService, ICacheService cacheService, ILogger<ResourceService> logger  )
        {
            _resourceRepository = resourceRepository;
            _metadataService = metadataService;
            _cacheService = cacheService;
            _logger = logger;
        }

        public IList<Uri> GetAllPidUris()
        {
            return _resourceRepository.GetAllPidUris();
        }

        public IList<VersionOverviewCTO> GetAllVersionsOfResourceByPidUri(Uri pidUri)
        {
            var versions = _resourceRepository.GetAllVersionsOfResourceByPidUri(pidUri);
            return versions;
        }

        public ResourcesCTO GetResourcesByPidUri(Uri pidUri)
        {
            if (!CheckIfResourceExist(pidUri, out var lifeCycleStatus))
            {
                return null;
            }
            var resources = _cacheService.GetOrAdd($"resources:{pidUri}", () => {
                var resourceTypes = _metadataService.GetInstantiableEntityTypes(Graph.Metadata.Constants.Resource.Type.FirstResouceType);
           
                var resourcesCTO = _resourceRepository.GetResourcesByPidUri(pidUri, resourceTypes);
                return resourcesCTO;
            }, TimeSpan.FromHours(1));
            return resources;
        }

        private bool CheckIfResourceExist(Uri pidUri, out string lifeCycleStatus)
        {
            return _resourceRepository.CheckIfResourceExist(pidUri, out lifeCycleStatus);
        }

        public void DeleteCachedResource(Uri pidUri)
        {
            _cacheService.Delete($"resources:{pidUri}");
        }

        public void DeleteCachedResources()
        {
            _cacheService.Delete("resources", "*");
        }


    }
}
