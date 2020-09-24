using System;
using System.Collections.Generic;
using System.Linq;
using COLID.IndexingCrawlerService.Repositories.Interface;
using COLID.IndexingCrawlerService.Services.Interface;
using COLID.Graph.Metadata.DataModels.Resources;

namespace COLID.IndexingCrawlerService.Services.Implementation
{
    public class ResourceService : IResourceService
    {
        private readonly IResourceRepository _resourceRepository;

        public ResourceService(IResourceRepository resourceRepository)
        {
            _resourceRepository = resourceRepository;
        }

        public bool CheckIfResourceExist(Uri pidUri, out string lifeCycleStatus)
        {
            return _resourceRepository.CheckIfResourceExist(pidUri, out lifeCycleStatus);
        }

        public IList<Uri> GetAllPublishedPidUris()
        {
            return _resourceRepository.GetAllPublishedPidUris();
        }

        public Resource GetPublishedResourceByPidUri(Uri pidUri)
        {
            if (!CheckIfResourceExist(pidUri, out var lifeCycleStatus))
            {
                return null;
            }

            return _resourceRepository.GetPublishedResourceByPidUri(pidUri);
        }

    }
}
