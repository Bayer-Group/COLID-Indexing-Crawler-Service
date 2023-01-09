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
using COLID.Graph.Metadata.Constants;
using COLID.Exception.Models.Business;

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
            var versions = _resourceRepository.GetAllVersionsOfResourceByPidUri(pidUri, null);
            return versions;
        }

        public ResourcesCTO GetResourcesByPidUri(Uri pidUri)
        {
            var resourceTypes = _metadataService.GetInstantiableEntityTypes(Graph.Metadata.Constants.Resource.Type.FirstResouceType);

            Uri instanceGraphUri = _metadataService.GetInstanceGraph(PIDO.PidConcept);
            Uri draftGraphUri = _metadataService.GetInstanceGraph("draft");
            //Initialize graphNames that should be sent to the repository
            Dictionary<Uri, bool> graphsToSearchIn = null;

            var graphExists = checkIfResourceExistAndReturnNamedGraph(pidUri, resourceTypes);
            bool draftExist = graphExists.GetValueOrDefault(draftGraphUri);
            bool publishedExist = graphExists.GetValueOrDefault(instanceGraphUri);
            
            graphsToSearchIn = new Dictionary<Uri, bool>();
            graphsToSearchIn.Add(draftGraphUri, draftExist);
            graphsToSearchIn.Add(instanceGraphUri, publishedExist);

            //if (!CheckIfResourceExist(pidUri, resourceTypes, draftGraphUri))
            //{
            //    return null;
            //}
            var resources = _cacheService.GetOrAdd($"resources:{pidUri}", () => {

                var resourcesCTO = _resourceRepository.GetResourcesByPidUri(pidUri, resourceTypes, graphsToSearchIn);
                if (resourcesCTO.HasDraft)
                {
                    resourcesCTO.Draft.Properties.TryGetValue(COLID.Graph.Metadata.Constants.Resource.HasLaterVersion, out List<dynamic> laterversionDraft);
                    if (laterversionDraft != null)
                    {
                        string oldValueDraft = laterversionDraft.FirstOrDefault();
                        var newLaterVersionD = oldValueDraft.Contains(COLID.Graph.Metadata.Constants.Entity.IdPrefix) ? _resourceRepository.GetPidUriById(new Uri(oldValueDraft), draftGraphUri, instanceGraphUri).ToString() : oldValueDraft;
                        laterversionDraft[0] = newLaterVersionD;
                    }
                }
                if (resourcesCTO.HasPublished)
                {
                    resourcesCTO.Published.Properties.TryGetValue(COLID.Graph.Metadata.Constants.Resource.HasLaterVersion, out List<dynamic> laterversionPublished);
                    if (laterversionPublished != null)
                    {
                        string oldValuePublished = laterversionPublished.FirstOrDefault();
                        var newLaterVersion = oldValuePublished.Contains(COLID.Graph.Metadata.Constants.Entity.IdPrefix) ? _resourceRepository.GetPidUriById(new Uri(oldValuePublished), draftGraphUri, instanceGraphUri).ToString() : oldValuePublished;
                        laterversionPublished[0] = newLaterVersion;
                    }
                }
                return resourcesCTO;
            }, TimeSpan.FromHours(1));
            return resources;
        }

        private bool CheckIfResourceExist(Uri pidUri, IList<string> resourceTypes, Uri namedGraph)
        {
            return _resourceRepository.CheckIfResourceExist(pidUri, resourceTypes, namedGraph);
        }

        public void DeleteCachedResource(Uri pidUri)
        {
            _cacheService.Delete($"resources:{pidUri}");
        }

        public void DeleteCachedResources()
        {
            _cacheService.Delete("resources", "*");
        }

        private Dictionary<Uri, bool> checkIfResourceExistAndReturnNamedGraph(Uri pidUri, IList<string> resourceTypes)
        {
            Uri instanceGraphUri = _metadataService.GetInstanceGraph(PIDO.PidConcept);
            Uri draftGraphUri = _metadataService.GetInstanceGraph("draft");

            var draftExist = _resourceRepository.CheckIfResourceExist(pidUri, resourceTypes, draftGraphUri);
            var publishExist = _resourceRepository.CheckIfResourceExist(pidUri, resourceTypes, instanceGraphUri);

            if (!draftExist && !publishExist)
            {
                throw new EntityNotFoundException("The requested resource does not exist in the database.",
                    pidUri.ToString());
            }

            var resourceExists = new Dictionary<Uri, bool>();
            resourceExists.Add(draftGraphUri, draftExist);
            resourceExists.Add(instanceGraphUri, publishExist);
            return resourceExists;
        }

    }
}
