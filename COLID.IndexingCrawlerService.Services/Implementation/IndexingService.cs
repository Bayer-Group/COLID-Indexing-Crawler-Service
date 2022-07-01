using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using System.Web;
using COLID.Cache.Services;
using COLID.Common.Extensions;
using COLID.Common.Utilities;
using COLID.Graph.HashGenerator.Services;
using COLID.Graph.Metadata.DataModels.MessageQueuing;
using COLID.Graph.Metadata.DataModels.Metadata;
using COLID.Graph.Metadata.DataModels.Resources;
using COLID.Graph.Metadata.Extensions;
using COLID.Graph.Metadata.Services;
using COLID.Graph.TripleStore.DataModels.Base;
using COLID.Graph.TripleStore.DataModels.Index;
using COLID.Graph.TripleStore.DataModels.Resources;
using COLID.Graph.TripleStore.DataModels.Taxonomies;
using COLID.Graph.TripleStore.Extensions;
using COLID.Identity.Extensions;
using COLID.Identity.Services;
using COLID.IndexingCrawlerService.Services.Configuration;
using COLID.IndexingCrawlerService.Services.Extensions;
using COLID.IndexingCrawlerService.Services.Interface;
using COLID.MessageQueue.Configuration;
using COLID.MessageQueue.Datamodel;
using COLID.MessageQueue.Services;
using CorrelationId.Abstractions;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;

namespace COLID.IndexingCrawlerService.Services.Implementation
{
    public class IndexingService : IIndexingService, IMessageQueueReceiver, IMessageQueuePublisher
    {
        private readonly CancellationToken _cancellationToken;
        private readonly ITokenService<ColidRegistrationServiceTokenOptions> _registrationServiceTokenService;
        private readonly ITokenService<ColidSearchServiceTokenOptions> _searchServiceTokenService;
        private readonly ILogger<IndexingService> _logger;
        private readonly IHttpClientFactory _clientFactory;
        private readonly IEntityHasher _hasher;
        private readonly ICacheService _cacheService;
        private readonly IMetadataService _metadataService;
        private readonly IResourceService _resourceService;
        private readonly IEntityService _entityService;
        private readonly ICorrelationContextAccessor _correlationContext;
        private readonly IConfiguration _configuration;
        private readonly ColidMessageQueueOptions _mqOptions;
        private readonly JsonSerializerSettings _serializerSettings;
        private readonly int LevelLinking = 2;

        private const string ResourceId = "resourceId";
        private const string InternalResourceId = "internalResourceId";
        private const string ResourceHash = "resourceHash";
        private const string ResourceLinkedLifecycleStatus = "resourceLinkedLifecycleStatus";

        public IDictionary<string, Action<string>> OnTopicReceivers => new Dictionary<string, Action<string>>() {
            {_mqOptions.Topics["ReindexingResources"], ReindexResource },
            {_mqOptions.Topics["IndexingResources"], IndexResourceFromTopic }
        };

        public Action<string, string, BasicProperty> PublishMessage { get; set; }

        public IndexingService(
            IOptionsMonitor<ColidMessageQueueOptions> messageQueueOptionsAccessor,
            ILogger<IndexingService> logger,
            IMetadataService metadataService,
            IResourceService resourceService,
            ICacheService cacheService,
            IEntityService entityService,
            IHttpClientFactory clientFactory,
            IEntityHasher hasher,
            ICorrelationContextAccessor correlationContext,
            IConfiguration configuration,
            ITokenService<ColidRegistrationServiceTokenOptions> registrationServiceTokenService,
            ITokenService<ColidSearchServiceTokenOptions> searchServiceTokenService,
            IHttpContextAccessor httpContextAccessor)
        {
            _mqOptions = messageQueueOptionsAccessor.CurrentValue;
            _logger = logger;
            _metadataService = metadataService;
            _resourceService = resourceService;
            _cacheService = cacheService;
            _entityService = entityService;
            _clientFactory = clientFactory;
            _correlationContext = correlationContext;
            _hasher = hasher;
            _configuration = configuration;
            _registrationServiceTokenService = registrationServiceTokenService;
            _searchServiceTokenService = searchServiceTokenService;

            _serializerSettings = new JsonSerializerSettings { ContractResolver = new CamelCasePropertyNamesContractResolver() };
            _cancellationToken = httpContextAccessor?.HttpContext?.RequestAborted ?? CancellationToken.None;
        }

        public async Task StartReindex()
        {
            _logger.LogInformation("[Reindexing] Start");
            var searchServiceIndexCreateUrl = $"{_configuration.GetConnectionString("searchServiceReindexUrl")}/api/index/create";

            _logger.LogInformation("[Reindexing] Clear cache");
            _cacheService.Clear();

            var metadataMapping = GetMetadataMappingForAllEntityTypes().ConfigureAwait(false).GetAwaiter().GetResult();

            using (var httpClient = _clientFactory.CreateClient())
            {
                _logger.LogInformation("[Reindexing] Sending metadata to search service: " + searchServiceIndexCreateUrl);

                var accessToken = await _searchServiceTokenService.GetAccessTokenForWebApiAsync();
                var response = await httpClient.SendRequestWithOptionsAsync(HttpMethod.Post, searchServiceIndexCreateUrl,
                    metadataMapping, accessToken, _cancellationToken, _correlationContext.CorrelationContext);

                if (!response.IsSuccessStatusCode)
                {
                    _logger.LogError("[Reindexing] Something went wrong while starting reindexing\nResponse Content={responseContent}", response.Content);
                    throw new System.Exception("Something went wrong while starting reindexing");
                }
            }

            _resourceService.DeleteCachedResources();
            SendAllResources();
        }

        /// <summary>
        /// Get the pid uri of all published entries and send it to mq
        /// </summary>
        private void SendAllResources()
        {
            _logger.LogInformation("[Reindexing] Send all resources");

            _logger.LogInformation("[Reindexing] Getting all resource pid uris");
            var pidUris = _resourceService.GetAllPidUris();

            _logger.LogInformation("[Reindexing] Sending {pidUrisCount} resources", pidUris.Count);

            var ind = 0;
            foreach (var pidUri in pidUris)
            {
                try
                {
                    _logger.LogInformation("[Reindexing] Sending PID URI to topic\nPidUri={PidUri}\nMqTopic={MQTopic}", pidUri, "TopicResourcePublishedPidUriIndexing");
                    PublishMessage(_mqOptions.Topics["ReindexingResources"], pidUri.ToString(), new BasicProperty() { Priority = 0 });
                    ind = ind + 1;
                    if (ind % 10 == 0)
                    {
                        _logger.LogInformation("Written " + ind + 1 + "resources to ReindxingResources Topic");
                    }
                }
                catch (System.Exception ex)
                {
                    _logger.LogError(ex, "[Reindexing] FAILED - Sending PID URI to topic\nPidUri={PidUri}\nMqTopic={MQTopic}", pidUri, "TopicResourcePublishedPidUriIndexing");
                }
            }
            _logger.LogInformation("[Reindexing] Actual count of resources sent to queue: " + (ind + 1));
            var lastPidUris= Enumerable.Reverse(pidUris).Take(5).Reverse().ToList();
            var message = JsonConvert.SerializeObject(new { lastPidUris = lastPidUris }, _serializerSettings);
            _logger.LogInformation("[Reindexing] Sending last5PidUris {message} to ReindexingSwitch Topic", message);
            try
            {
                PublishMessage(_mqOptions.Topics["ReindexingSwitch"], message, new BasicProperty() { Priority = 0 });
            }
            catch (System.Exception ex)
            {
                _logger.LogError(ex, "[Reindexing] Something went wrong while sending the last5PidUris.\nPidUri={lastPidUri}\nMqTopic=ReindexingSwitch", message);
            }
        }

        public void IndexResource(ResourceIndexingDTO resourceIndexingDto)
        {
            Guard.ArgumentNotNull(resourceIndexingDto, nameof(resourceIndexingDto));

            try
            {
                _logger.LogInformation("[Indexing] Start indexing for pid uri {pidUri} with action {action}",
                resourceIndexingDto.PidUri, resourceIndexingDto.Action);

                switch (resourceIndexingDto.Action)
                {
                    case ResourceCrudAction.Reindex:
                        IndexResource(resourceIndexingDto, false, false);
                        break;
                    case ResourceCrudAction.Deletion:
                        _resourceService.DeleteCachedResource(resourceIndexingDto.PidUri);
                        DeleteResource(resourceIndexingDto, true);
                        break;
                    default:
                        _resourceService.DeleteCachedResource(resourceIndexingDto.PidUri);
                        IndexResource(resourceIndexingDto, true, true);
                        break;
                }
            }
            catch (System.Exception ex)
            {
                _logger.LogError(ex, "[Indexing][New Resource] An error occurred during indexing process");
                throw;
            }
        }

        public void IndexResourceFromTopic(string indexedResourceString)
        {
            _logger.LogInformation($"[Indexing] Receive indexed resource from Topic 'IndexingResources'");

            Guard.ArgumentNotNullOrWhiteSpace(indexedResourceString, nameof(indexedResourceString));

            try
            {
                _logger.LogInformation("[Indexing] Message to be index from reg service is {message}", indexedResourceString);
                var resourceIndexingDto = JsonConvert.DeserializeObject<ResourceIndexingDTO>(indexedResourceString);
                IndexResource(resourceIndexingDto);
            }
            catch (JsonSerializationException)
            {
                _logger.LogError("[Indexing][New Resource] An error occurred during deserialization");
                throw;
            }
        }

        /// <summary>
        /// Create the indexing document for the give resource.
        /// This document is sent to the search service by the message que
        /// </summary>
        /// <param name="resourceIndexingDto">Resource indexing dto to be indexed</param>
        /// <param name="updatedInboundLinks">Indicates whether resources that have outgoing connections to the current resource need to be updated . </param>
        /// <param name="updateOutboundLinks">indicates whether linked resources of the current resource must be updated.</param>
        private void IndexResource(ResourceIndexingDTO resourceIndexingDto, bool updatedInboundLinks, bool updateOutboundLinks)
        {
            string resourceType = resourceIndexingDto.Resource.Properties.GetValueOrNull(Graph.Metadata.Constants.RDF.Type, true);
            var metadataProperties = _metadataService.GetMetadataForEntityType(resourceType);

            IndexResource(resourceIndexingDto, metadataProperties);

            // If a draft resource is updated or re-indexed and also has a published resource, the published resource must also be indexed. 
            if ((resourceIndexingDto.Action == ResourceCrudAction.Update || resourceIndexingDto.Action == ResourceCrudAction.Reindex) &&
                resourceIndexingDto.CurrentLifecycleStatus == Graph.Metadata.Constants.Resource.ColidEntryLifecycleStatus.Draft &&
                resourceIndexingDto.RepoResources.HasPublished)
            {
                var linkedResource = resourceIndexingDto.RepoResources.Published;
                var repoResources = new ResourcesCTO(resourceIndexingDto.Resource, resourceIndexingDto.RepoResources.Published, resourceIndexingDto.RepoResources.Versions);
                var linkedResourceIndexingDto = new ResourceIndexingDTO(ResourceCrudAction.Update, resourceIndexingDto.PidUri, linkedResource, repoResources);

                // Links do not need to be updated, because only the information that the published entry has a draft is relevant and must be updated.
                IndexResource(linkedResourceIndexingDto, metadataProperties);
            }

            // If a resource is published and also has a draft version, the draft version must be removed from the index.  
            if (resourceIndexingDto.Action == ResourceCrudAction.Publish &&
                resourceIndexingDto.RepoResources.HasDraft)
            {
                var linkedResource = resourceIndexingDto.RepoResources.Draft;
                var linkedResourceIndexingDto = new ResourceIndexingDTO(ResourceCrudAction.Update, resourceIndexingDto.PidUri, linkedResource, resourceIndexingDto.RepoResources);

                DeleteResource(linkedResourceIndexingDto, false);
            }

            // Links must be updated in different cases, because they contain changed information of the current resource 
            if (updatedInboundLinks || updateOutboundLinks)
            {
                UpdateLinks(resourceIndexingDto, metadataProperties, updatedInboundLinks, updateOutboundLinks);
            }
        }

        private void IndexResource(ResourceIndexingDTO resourceIndexingDto, IList<MetadataProperty> metadataProperties)
        {
            if (resourceIndexingDto.Resource == null || !metadataProperties.Any())
            {
                _logger.LogInformation("[Indexing] Send published resource. No resource or no metatdata\nPidUri={PidUri}\nMqTopic={MQTopic}", resourceIndexingDto.PidUri, "TopicResourcePublished");
                return;
            }

            try
            {
                var indexDocument = GenerateIndexDocument(resourceIndexingDto, metadataProperties);
                var message = JsonConvert.SerializeObject(indexDocument, _serializerSettings);

                _logger.LogInformation("[Indexing] Publish mq message for resource.\nPidUri={PidUri}\nMqTopic={MQTopic}", resourceIndexingDto.PidUri, "TopicResourcePublished");
                _logger.LogInformation("[Indexing] Message to be published is {message}", message);
                PublishMessage(_mqOptions.Topics["IndexingResourceDocument"], message, new BasicProperty() { Priority = 0 });
            }
            catch (System.Exception ex)
            {
                _logger.LogError(ex, "[Indexing] Something went wrong while sending the resource.\nPidUri={PidUri}\nMqTopic={MQTopic}", resourceIndexingDto.PidUri, "TopicResourcePublished");
            }
        }

        /// <summary>
        /// Deletes a resource from the corresponding index.
        /// For draft versions the published resource is updated in addition to the links. 
        /// </summary>
        /// <param name="resourceIndexingDto">Resource indexing dto to be deleted</param>
        /// <param name="updatePublished">Determines is published should be updated</param>
        private void DeleteResource(ResourceIndexingDTO resourceIndexingDto, bool updatePublished)
        {
            Guard.ArgumentNotNull(resourceIndexingDto, nameof(resourceIndexingDto));

            var mqMessageDict = new Dictionary<string, MessageQueuePropertyDTO>();

            var resourceIdMqProperty = GenerateResourceIdMessageQueueProperty(resourceIndexingDto.PidUri);
            mqMessageDict.Add(ResourceId, resourceIdMqProperty);

            // 1. Delete draft while publishing (updatePublished = false) -> update outbound
            // 2. delete draft (updatePublished = true) -> update inbound and outbound
            // 3. Delete published -> update inbound and outbound 
            try
            {
                var currentLifeCycle = resourceIndexingDto.Resource.Properties.GetValueOrNull(Graph.Metadata.Constants.Resource.HasEntryLifecycleStatus, true);
                var indexDocument = new IndexDocumentDto(resourceIndexingDto.PidUri, ResourceCrudAction.Deletion, currentLifeCycle, mqMessageDict);
                var message = JsonConvert.SerializeObject(indexDocument, _serializerSettings);

                PublishMessage(_mqOptions.Topics["IndexingResourceDocument"], message, new BasicProperty() { Priority = 0 });

                string resourceType = resourceIndexingDto.Resource.Properties.GetValueOrNull(Graph.Metadata.Constants.RDF.Type, true);
                var metadataProperties = _metadataService.GetMetadataForEntityType(resourceType);

                // If a published resource exists and the draft should be deleted, the published resource must be updated, otherwise only links will be updated
                if (currentLifeCycle == COLID.Graph.Metadata.Constants.Resource.ColidEntryLifecycleStatus.Draft &&
                    resourceIndexingDto.RepoResources.HasPublished)
                {
                    if (updatePublished)
                    {
                        var repoResources = new ResourcesCTO(null, resourceIndexingDto.RepoResources.Published, resourceIndexingDto.RepoResources.Versions);
                        var linkedResourceIndexingDto = new ResourceIndexingDTO(ResourceCrudAction.Publish, resourceIndexingDto.PidUri, resourceIndexingDto.RepoResources.Published, repoResources);

                        // Update published outbound links and draft & published related inbound links
                        IndexResource(linkedResourceIndexingDto, true, true);
                    }

                    // Update draft outbound links
                    UpdateLinks(resourceIndexingDto, metadataProperties, false, true);
                }
                else
                {
                    UpdateLinks(resourceIndexingDto, metadataProperties, true, true);
                }

            }
            catch (System.Exception ex)
            {
                _logger.LogError(ex, "[Indexing] FAILED Deleting resource.\nPidUri={PidUri}\nMqTopic={MQTopic}", resourceIndexingDto.PidUri, "TopicResourceDeleted");
            }
        }

        /// <summary>
        /// Aggregates all links that go out from the resource and enter the resource.
        /// As these resources provide information, these resources are updated.
        /// The difference is that the links of the linked resource do not need to be updated. 
        /// </summary>
        /// <param name="resourceIndexingDto">Indexed resource</param>
        /// <param name="metadataProperties">Metadata of current resource to be indexed</param>
        /// <param name="inbound">Indicates whether resources that have outgoing connections to the current resource need to be updated . </param>
        /// <param name="outbound">indicates whether linked resources of the current resource must be updated.</param>
        private void UpdateLinks(ResourceIndexingDTO resourceIndexingDto, IList<MetadataProperty> metadataProperties, bool inbound, bool outbound)
        {
            var pidUri = resourceIndexingDto.PidUri;

            ISet<string> links = new HashSet<string>();

            // All linked resources, the current resource as well as the links that may have been removed are extracted.  
            if (outbound)
            {
                links.AddRange(resourceIndexingDto.Resource.GetLinks(metadataProperties));

                if (resourceIndexingDto.RepoResources.HasDraft)
                {
                    links.AddRange(resourceIndexingDto.RepoResources.Draft.GetLinks(metadataProperties));
                }

                if (resourceIndexingDto.RepoResources.HasPublished)
                {
                    links.AddRange(resourceIndexingDto.RepoResources.Published.GetLinks(metadataProperties));
                }
            }

            // All resources that have a link to the current resource as well as resources in a version chain are extracted.
            if (inbound)
            {
                var versionedPidUris = resourceIndexingDto.RepoResources.Versions.Select(t => t.PidUri);

                links.AddRange(versionedPidUris);

                var inboundLinks = resourceIndexingDto.InboundProperties;
                var inboundPidUris = inboundLinks.SelectMany(v => v.Value).Cast<string>().ToHashSet();

                links.AddRange(inboundPidUris);
            }

            // With draft resources there is an incoming link to the published resource, so this pid uri must be removed. 
            if (links.Contains(pidUri.ToString()))
            {
                links.Remove(pidUri.ToString());
            }

            // For each pid uri the resource is fetched from the triplestore and will be updated. Further links are ignored and not updated.
            //_resourceService.DeleteCachedResource(pidUri);
            foreach (var linkedPidUriString in links)
            {
                var linkedPidUri = new Uri(linkedPidUriString);
                _resourceService.DeleteCachedResource(linkedPidUri);
                var linkedResource = _resourceService.GetResourcesByPidUri(linkedPidUri);
                foreach (var kvp in linkedResource.Published.InboundProperties)
                {
                    _logger.LogInformation("[UpdateLinks] Inbound Props for child resource are {kvp.Key} and {kvp.Value}", kvp.Key, kvp.Value);
                }
                var linkedResourceIndexingDto = new ResourceIndexingDTO(ResourceCrudAction.Update, linkedPidUri, linkedResource.GetDraftOrPublishedVersion(), linkedResource);
                _logger.LogInformation("[UpdateLinks] linkedPidUri to index is {linkedPidUri}", linkedPidUri);
                _logger.LogInformation("[Indexing] Message to be index with piduri is {linkedPidUri}", linkedPidUri);
                _logger.LogInformation("[Indexing] Message to be index from update link is {message}", JsonConvert.SerializeObject(linkedResourceIndexingDto));
                var resourceString = JsonConvert.SerializeObject(linkedResource);
                _logger.LogInformation("[Indexing] Message to be index from update link is {resourceString}", resourceString);
                IndexResource(linkedResourceIndexingDto, false, false);
            }
        }

        /// <summary>
        /// Send the resource to the mq
        /// </summary>
        /// <param name="pidUriString">Pid uri of resource</param>
        public void ReindexResource(string pidUriString)
        {
            _logger.LogInformation("[Reindex] Receive pid uri {pidUri} from Topic 'ReindexingResources'", pidUriString);

            Guard.ArgumentNotNullOrWhiteSpace(pidUriString, nameof(pidUriString));

            try
            {
                var pidUri = new Uri(pidUriString);
                var resources = _resourceService.GetResourcesByPidUri(pidUri);
                
                var resourceIndexingDto = new ResourceIndexingDTO(ResourceCrudAction.Reindex, pidUri, resources.GetDraftOrPublishedVersion(), resources);
                IndexResource(resourceIndexingDto);
            }
            catch (System.Exception ex)
            {
                _logger.LogError(ex, $"Something went wrong while reindex resource {pidUriString} by message queue");
            }
        }

        private IndexDocumentDto GenerateIndexDocument(ResourceIndexingDTO resourceIndexingDto, IList<MetadataProperty> metadataProperties)
        {
            var currentLifeCycle = resourceIndexingDto.Resource.Properties.GetValueOrNull(Graph.Metadata.Constants.Resource.HasEntryLifecycleStatus, true);

            _logger.LogInformation("[Indexing] Generating mq message for resource\nPidUri={PidUri}", resourceIndexingDto.PidUri);

            var linkingLevelIndexer = 0;
            var mqMessageDict = GenerateMqMessage(resourceIndexingDto, resourceIndexingDto.Resource, metadataProperties, linkingLevelIndexer);

            var inboundLinkingLevelIndexer = 0;
            var mqMessageDictInbound = GenerateMqMessage(resourceIndexingDto, resourceIndexingDto.Resource, metadataProperties, inboundLinkingLevelIndexer, true);

            CombineInboundAndOutboundProperties(mqMessageDict, mqMessageDictInbound);

            AddAdditionalMqProperties(resourceIndexingDto, mqMessageDict, metadataProperties);

            _logger.LogInformation("[Indexing] Generated mq message for resource\nPidUri={PidUri}", resourceIndexingDto.PidUri);

            var indexDocument = new IndexDocumentDto(resourceIndexingDto.PidUri, resourceIndexingDto.Action, currentLifeCycle, mqMessageDict);

            return indexDocument;
        }

        /// <summary>
        /// Adds further properties to the document, which are relevant for the index.
        /// </summary>
        /// <param name="resource">resource to add additional properties for</param>
        /// <param name="mqMessageDict">Dictionary with all normal resource properties</param>
        private void AddAdditionalMqProperties(ResourceIndexingDTO resourceIndexingDto, IDictionary<string, MessageQueuePropertyDTO> mqMessageDict, IList<MetadataProperty> metadataProperties)
        {
            var versionMqProperty = GenerateVersionMqProperty(resourceIndexingDto.PidUri, resourceIndexingDto);
            mqMessageDict.Add(Graph.Metadata.Constants.Resource.HasVersions, versionMqProperty);

            var resourceIdMqProperty = GenerateResourceIdMessageQueueProperty(resourceIndexingDto.PidUri);
            mqMessageDict.Add(ResourceId, resourceIdMqProperty);

            var internalResourceIdMqProperty = GenerateInternalResourceIdMessageQueueProperty(resourceIndexingDto.Resource);
            mqMessageDict.Add(InternalResourceId, internalResourceIdMqProperty);

            var internalResourceHashMqProperty = GenerateResourceHashMessageQueueProperty(resourceIndexingDto.Resource, mqMessageDict, metadataProperties);
            mqMessageDict.Add(ResourceHash, internalResourceHashMqProperty);

            if (GeneratePointsAtMessageQueueProperty(resourceIndexingDto.Resource, out var pointsAtMqProperty))
            {
                mqMessageDict.Add(Graph.Metadata.Constants.Resource.PointAt, pointsAtMqProperty);
            }

            if (GenerateResourceLinkedEntryLifeCycleMessageQueueProperty(resourceIndexingDto, out var lifecycleStatusMqProperty))
            {
                mqMessageDict.Add(ResourceLinkedLifecycleStatus, lifecycleStatusMqProperty);
            }
        }

        #region Additional Mq Properties

        /// <summary>
        /// Generates an mq property for the versions. all previous versions are stored as inbound values and all older versions are stored as outbound values.
        /// </summary>
        /// <param name="resource">The resource from the version chain that is to be updated</param>
        /// <returns>Returns an mq property for the versions chain</returns>
        private MessageQueuePropertyDTO GenerateVersionMqProperty(Uri pidUri, ResourceIndexingDTO resourceIndexingDto)
        {
            var versions = resourceIndexingDto.RepoResources.Versions;
            string actualVersion = resourceIndexingDto.Resource.Properties.GetValueOrNull(Graph.Metadata.Constants.Resource.HasVersion, true);

            var messageQueueProperty = new MessageQueuePropertyDTO();

            foreach (var version in versions)
            {
                // TODO: Not for save action
                if (resourceIndexingDto.CurrentLifecycleStatus != Graph.Metadata.Constants.Resource.ColidEntryLifecycleStatus.Draft && !CheckVersionHasPublishedVersion(version))
                {
                    continue;
                }

                var resourceId = version.Id;
                var value = new Dictionary<string, MessageQueueDirectionPropertyDTO>
                {
                    {
                        Graph.Metadata.Constants.EnterpriseCore.PidUri,
                        new MessageQueueDirectionPropertyDTO(version.PidUri, version.PidUri)
                    },
                    {
                        Graph.Metadata.Constants.Resource.BaseUri,
                        new MessageQueueDirectionPropertyDTO(version.BaseUri, version.BaseUri)
                    },
                    {
                        Graph.Metadata.Constants.Resource.HasVersion,
                        new MessageQueueDirectionPropertyDTO(version.Version, null)
                    }
                };

                var messageQueueDirectionProperty = new MessageQueueDirectionPropertyDTO(value, resourceId, Graph.Metadata.Constants.Resource.HasVersions);

                if (actualVersion.CompareVersionTo(version.Version) > 0)
                {
                    messageQueueProperty.Inbound.Add(messageQueueDirectionProperty);
                }

                if (actualVersion.CompareVersionTo(version.Version) < 0)
                {
                    messageQueueProperty.Outbound.Add(messageQueueDirectionProperty);
                }
            }

            return messageQueueProperty;
        }

        private static bool CheckVersionHasPublishedVersion(VersionOverviewCTO versionOverview)
        {
            return versionOverview.LifecycleStatus == Graph.Metadata.Constants.Resource.ColidEntryLifecycleStatus.Published ||
                versionOverview.LifecycleStatus == Graph.Metadata.Constants.Resource.ColidEntryLifecycleStatus.MarkedForDeletion ||
                   !string.IsNullOrWhiteSpace(versionOverview.PublishedVersion);
        }

        /// <summary>
        /// For the main distribution endpoint, a separate mq property is created that points to the endpoint.
        /// Main distribution endpoints are listed in the index document as normal endpoints.
        /// </summary>
        /// <param name="resource">The resource used to generate the property</param>
        /// <returns>Returns an mq property</returns>
        private static bool GeneratePointsAtMessageQueueProperty(Entity resource, out MessageQueuePropertyDTO mqProperty)
        {
            Entity mainDistribution = resource.Properties.GetValueOrNull(Graph.Metadata.Constants.Resource.MainDistribution, true);
            if (mainDistribution != null)
            {
                var propertyDTO = new MessageQueueDirectionPropertyDTO(null, mainDistribution.Id, Graph.Metadata.Constants.Resource.PointAt);
                mqProperty = new MessageQueuePropertyDTO() { Outbound = new List<MessageQueueDirectionPropertyDTO>() { propertyDTO } };
                return true;
            }

            mqProperty = null;
            return false;
        }

        /// <summary>
        /// Generates the id of the index document as mq property. The PID URI is used as id.
        /// </summary>
        /// <param name="pidUri">PID URI of the pid entry for which an index document is generated</param>
        /// <returns>Returns an mq property</returns>
        private static MessageQueuePropertyDTO GenerateResourceIdMessageQueueProperty(Uri pidUri)
        {
            return new MessageQueuePropertyDTO() { Outbound = new List<MessageQueueDirectionPropertyDTO>() { new MessageQueueDirectionPropertyDTO(null, pidUri.ToString()) } };
        }

        /// <summary>
        /// Generates a mq property for internal identifier.
        /// </summary>
        /// <param name="resource">The resource used to generate the property</param>
        /// <returns>Returns an mq property</returns>
        private static MessageQueuePropertyDTO GenerateInternalResourceIdMessageQueueProperty(Entity resource)
        {
            return new MessageQueuePropertyDTO() { Outbound = new List<MessageQueueDirectionPropertyDTO>() { new MessageQueueDirectionPropertyDTO(null, resource.Id) } };
        }

        /// <summary>
        /// Generates a mq property for hashed resources.
        /// </summary>
        /// <param name="resource">The resource used to generate the hash</param>
        /// <returns>Returns an mq property</returns>
        private MessageQueuePropertyDTO GenerateResourceHashMessageQueueProperty(Entity resource, IDictionary<string, MessageQueuePropertyDTO> mqMessageDict, IList<MetadataProperty> metadataProperties)
        {
            IEnumerable<dynamic> keywordLabels = new List<dynamic>();

            foreach (var metadataProperty in metadataProperties)
            {
                if (metadataProperty.IsControlledVocabulary(out var range) && metadataProperty.Properties.TryGetValue(COLID.Graph.Metadata.Constants.PIDO.Shacl.FieldType, out var fieldType))
                {
                    if (fieldType == COLID.Graph.Metadata.Constants.PIDO.Shacl.FieldTypes.ExtendableList)
                    {
                        if (mqMessageDict.TryGetValue(metadataProperty.Key, out var value))
                        {
                            keywordLabels = value.Outbound.Select(t => t.Value);
                        }
                    }
                }
            }

            var resourceCopy = resource;

            if (keywordLabels != null && keywordLabels.Any())
            {
                resourceCopy = new Entity { Id = resource.Id, InboundProperties = resource.InboundProperties, Properties = resource.Properties };
                resourceCopy.Properties[Graph.Metadata.Constants.Resource.Keyword] = keywordLabels.ToList();
            }

            var sha256Hash = _hasher.Hash(resourceCopy);

            return new MessageQueuePropertyDTO() { Outbound = new List<MessageQueueDirectionPropertyDTO> { new MessageQueueDirectionPropertyDTO(sha256Hash, null) } };
        }

        /// <summary>
        /// Generates a mq property to define if a resource with antoher lifecycle status exists..
        /// </summary>
        /// <param name="resource">The resource used to be checked</param>
        /// <returns>Returns an mq property</returns>
        private bool GenerateResourceLinkedEntryLifeCycleMessageQueueProperty(ResourceIndexingDTO resourceIndexingDto, out MessageQueuePropertyDTO mqProperty)
        {
            var currentLifeCycle = resourceIndexingDto.CurrentLifecycleStatus;

            if (currentLifeCycle == COLID.Graph.Metadata.Constants.Resource.ColidEntryLifecycleStatus.Draft && resourceIndexingDto.RepoResources.HasPublished)
            {
                var publishedResourceStatus =
                    resourceIndexingDto.RepoResources.Published.Properties.GetValueOrNull(
                        COLID.Graph.Metadata.Constants.Resource.HasEntryLifecycleStatus, true);
                mqProperty = new MessageQueuePropertyDTO() { Outbound = new List<MessageQueueDirectionPropertyDTO> { new MessageQueueDirectionPropertyDTO(null, publishedResourceStatus) } };
                return true;
            }

            if (currentLifeCycle != COLID.Graph.Metadata.Constants.Resource.ColidEntryLifecycleStatus.Draft && resourceIndexingDto.RepoResources.HasDraft && resourceIndexingDto.Action != ResourceCrudAction.Publish)
            {
                var draftResourceStatus =
                    resourceIndexingDto.RepoResources.Draft.Properties.GetValueOrNull(
                        COLID.Graph.Metadata.Constants.Resource.HasEntryLifecycleStatus, true);
                mqProperty = new MessageQueuePropertyDTO() { Outbound = new List<MessageQueueDirectionPropertyDTO> { new MessageQueueDirectionPropertyDTO(null, draftResourceStatus) } };
                return true;
            }

            mqProperty = null;
            return false;
        }

        #endregion

        /// <summary>
        /// Adds the inbound left to the outbounds left so that all left are clustered into a key.
        /// </summary>
        /// <param name="mqMessageDict">Dictionary with all normal properties</param>
        /// <param name="mqMessageDictInbound">Dictionary with all linked inbound properties</param>
        private static void CombineInboundAndOutboundProperties(IDictionary<string, MessageQueuePropertyDTO> mqMessageDict, IDictionary<string, MessageQueuePropertyDTO> mqMessageDictInbound)
        {
            foreach (var message in mqMessageDictInbound)
            {
                if (mqMessageDict.TryGetValue(message.Key, out var property))
                {
                    property.Inbound.AddRange(message.Value.Inbound);
                }
                else
                {
                    mqMessageDict.Add(message.Key, new MessageQueuePropertyDTO() { Inbound = message.Value.Inbound });
                }
            }
        }

        private IDictionary<string, MessageQueuePropertyDTO> GenerateMqMessage(ResourceIndexingDTO resourceIndexingDto, Entity entity, IList<MetadataProperty> metadataProperties, int linkingLevelIndexer, bool inbound = false)
        {
            var mqMessageDict = new Dictionary<string, MessageQueuePropertyDTO>();

            IDictionary<string, List<dynamic>> resourceProperties;
            if (inbound)
            {
                resourceProperties = resourceIndexingDto.InboundProperties;
            }
            else
            {
                resourceProperties = entity.Properties;
            }

            if (resourceProperties == null || metadataProperties == null)
            {
                return mqMessageDict;
            }

            foreach (var propertyItem in resourceProperties)
            {
                var metadataProperty = metadataProperties.FirstOrDefault(metaProp => metaProp.Properties.GetValueOrNull(Graph.Metadata.Constants.EnterpriseCore.PidUri, true) == propertyItem.Key);

                // Main distribution is an ignored property, but must be handled separately at this point, as it is written to the index as a normal endpoint.
                if (IsIgnoredMetadataProperty(propertyItem.Key, metadataProperty, inbound) && Graph.Metadata.Constants.Resource.MainDistribution != metadataProperty?.Key)
                {
                    continue;
                }

                foreach (var property in propertyItem.Value)
                {
                    if (property == null)
                    {
                        continue;
                    }

                    var newLinkingLevelIndexer = linkingLevelIndexer;

                    Entity inboundEntity = null;

                    // in some cases, the inbound type may not be the same as the current entity type, missing metadata must be loaded
                    if (metadataProperty == null && inbound)
                    {
                        inboundEntity = GetLinkedResource(resourceIndexingDto, property);

                        if (inboundEntity != null)
                        {
                            var inboundEntityType = inboundEntity.Properties.GetValueOrNull(Graph.Metadata.Constants.RDF.Type, true);

                            IList<MetadataProperty> newMetadataProperties = _metadataService.GetMetadataForEntityType(inboundEntityType);
                            metadataProperty = newMetadataProperties?.FirstOrDefault(metaProp => metaProp.Properties.GetValueOrNull(Graph.Metadata.Constants.EnterpriseCore.PidUri, true) == propertyItem.Key);
                        }
                    }

                    var propertyValue = inboundEntity ?? property;

                    if (metadataProperty != null)
                    {
                        var mqProperty = GenerateMqDirectionProperty(resourceIndexingDto, propertyItem.Key, propertyValue, metadataProperty, newLinkingLevelIndexer);

                        if (mqProperty != null)
                        {
                            string key;
                            if (metadataProperty.GetMetadataPropertyGroup()?.Key == Graph.Metadata.Constants.Resource.Groups.LinkTypes)
                            {
                                key = Graph.Metadata.Constants.Resource.Groups.LinkTypes;
                            }
                            else if (propertyItem.Key == Graph.Metadata.Constants.Resource.MainDistribution)
                            {
                                key = Graph.Metadata.Constants.Resource.Distribution;
                                mqProperty.Edge = Graph.Metadata.Constants.Resource.Distribution;
                            }
                            else
                            {
                                key = propertyItem.Key;
                            }

                            if (mqMessageDict.TryGetValue(key, out MessageQueuePropertyDTO mqObject))
                            {
                                if (mqObject is MessageQueuePropertyDTO mqProp)
                                {
                                    if (inbound) { mqProp.Inbound.Add(mqProperty); } else { mqProp.Outbound.Add(mqProperty); };
                                }
                            }
                            else
                            {
                                if (inbound)
                                {
                                    mqMessageDict.Add(key, new MessageQueuePropertyDTO() { Inbound = new List<MessageQueueDirectionPropertyDTO>() { mqProperty } });
                                }
                                else
                                {
                                    mqMessageDict.Add(key, new MessageQueuePropertyDTO() { Outbound = new List<MessageQueueDirectionPropertyDTO>() { mqProperty } });
                                }
                            }
                        }
                    }
                }
            }

            return mqMessageDict;
        }

        private MessageQueueDirectionPropertyDTO GenerateMqDirectionProperty(ResourceIndexingDTO resourceIndexingDto, string propertyKey, dynamic propertyValue, MetadataProperty metadataProperty, int linkingLevelIndexer)
        {
            var metadataGroup = metadataProperty.GetMetadataPropertyGroup();

            if (metadataGroup?.Key == Graph.Metadata.Constants.Resource.Groups.LinkTypes && propertyValue != null && !DynamicExtension.IsType<Entity>(propertyValue, out Entity linkedEntity))
            {
                linkedEntity = GetLinkedResource(resourceIndexingDto, propertyValue);

                if (linkedEntity != null)
                {
                    propertyValue = linkedEntity;
                }
            }


            if (DynamicExtension.IsType<Entity>(propertyValue, out Entity nestedEntity))
            {
                linkingLevelIndexer++;

                string pidUri = nestedEntity.GetType().GetProperty("PidUri") == null ? nestedEntity.Id :
                   ((System.Uri)nestedEntity.GetType().GetProperty("PidUri").GetValue(nestedEntity, null)).AbsoluteUri;

                var mqProperty = new MessageQueueDirectionPropertyDTO(nestedEntity.Id, pidUri, propertyKey);

                string typeOfNestedEntity = nestedEntity.Properties.GetValueOrNull(Graph.Metadata.Constants.RDF.Type, true);
                if (typeOfNestedEntity == null || string.IsNullOrWhiteSpace(typeOfNestedEntity))
                {
                    _logger.LogWarning($"No type predicate for nested entity {JsonConvert.SerializeObject(nestedEntity)}.");
                }
                if (metadataProperty.NestedMetadata == null)
                {
                    _logger.LogWarning($"No nested metadata for nested entity {JsonConvert.SerializeObject(nestedEntity)}.");
                }

                Metadata metadataForNestedEntityType = null;

                var entryLifeCycleStatus = nestedEntity.Properties.GetValueOrNull(Graph.Metadata.Constants.Resource.HasEntryLifecycleStatus, true);

                var removeLinkedDraftResources = resourceIndexingDto.CurrentLifecycleStatus !=
                                                                        COLID.Graph.Metadata.Constants.Resource.ColidEntryLifecycleStatus.Draft;

                if (removeLinkedDraftResources && !string.IsNullOrWhiteSpace(entryLifeCycleStatus) &&
                    entryLifeCycleStatus == Graph.Metadata.Constants.Resource.ColidEntryLifecycleStatus.Draft)
                {
                    return null;
                }

                if (metadataGroup?.Key != Graph.Metadata.Constants.Resource.Groups.LinkTypes && linkingLevelIndexer < this.LevelLinking)
                {
                    metadataForNestedEntityType = metadataProperty.NestedMetadata?.Where(m => m.Key == typeOfNestedEntity).FirstOrDefault();
                }
                else if (metadataGroup?.Key == Graph.Metadata.Constants.Resource.Groups.LinkTypes && linkingLevelIndexer < this.LevelLinking)
                {
                    metadataForNestedEntityType = new Metadata(typeOfNestedEntity, null, null, _metadataService.GetMetadataForEntityType(typeOfNestedEntity));
                }

                if (metadataForNestedEntityType == null)
                {
                    _logger.LogWarning($"No nested metadata for nested entity type {typeOfNestedEntity}.");
                }
                else
                {
                    mqProperty.Value = GenerateMqMessage(resourceIndexingDto, nestedEntity, metadataForNestedEntityType.Properties, linkingLevelIndexer);
                }

                return mqProperty;
            }
            else if (propertyValue is DateTime)
            {
                return new MessageQueueDirectionPropertyDTO(propertyValue.ToString("o", DateTimeFormatInfo.InvariantInfo), null);
            }
            else if (metadataProperty.IsControlledVocabulary(out var range) || propertyKey == Graph.Metadata.Constants.RDF.Type)
            {
                var id = propertyValue as string;

                try
                {
                    var entity = _entityService.GetEntity(id);

                    if (entity == null)
                    {
                        _logger.LogWarning(
                            $"Unable to map CV for property with key={propertyKey} and value={propertyValue}.");
                        return null;
                    }

                    return new MessageQueueDirectionPropertyDTO(entity.Name, id);
                }
                catch (System.Exception exception)
                {
                    _logger.LogError(exception,
                        $"Unable to receive entity for property with key={propertyKey} and value={propertyValue}.");
                    return null;
                }
                
            }
            else
            {
                return new MessageQueueDirectionPropertyDTO(Convert.ToString(propertyValue), null);
            }
        }

        private Entity GetLinkedResource(ResourceIndexingDTO resourceIndexingDto, dynamic value)
        {
            Uri linkedPidURi = null;

            if (Uri.TryCreate(value, UriKind.Absolute, out linkedPidURi))
            {
                var resources = _resourceService.GetResourcesByPidUri(linkedPidURi);

                if (resources != null && resources.HasPublishedOrDraft)
                {
                    var inboundEntity =
                        resourceIndexingDto.CurrentLifecycleStatus == COLID.Graph.Metadata.Constants.Resource
                            .ColidEntryLifecycleStatus.Draft
                            ? resources.GetDraftOrPublishedVersion()
                            : resources.GetPublishedOrDraftVersion();
                    return inboundEntity;
                }
            }

            return null;
        }

        /// <summary>
        /// Returns a flat list of all metadata properties of all types. It adds instances as controlled vocabulary and deletes main distribution endpoint property.
        /// </summary>
        /// <returns></returns>
        private async Task<IDictionary<string, MetadataProperty>> GetMetadataMappingForAllEntityTypes()
        {
            var resourceTypes = _metadataService.GetInstantiableEntityTypes(Graph.Metadata.Constants.Resource.Type.FirstResouceType);
            var metadataMappingList = _metadataService.GetMergedMetadata(resourceTypes);
            metadataMappingList = metadataMappingList.ModifyPropertiesForElastic();

            _logger.LogInformation("[Indexing] Setup metadata mapping for all entity types");

            var metadataDict = metadataMappingList.Select(async metadataProperty =>
            {
                // TODO: If own shacls types are implemented only fetch taxonomies instead of all CVs
                if (metadataProperty.IsControlledVocabulary(out var range) || metadataProperty.Key == Graph.Metadata.Constants.RDF.Type)
                {
                    IList<TaxonomyResultDTO> taxonomies;

                    using (var httpClient = _clientFactory.CreateClient())
                    {
                        _logger.LogInformation("[Indexing] Setup metadata mapping: Start retrieving taxonomies");
                        var encodedRange = HttpUtility.UrlEncode(range);
                        var registrationServiceGetTaxonomyListUrl = $"{_configuration.GetConnectionString("colidRegistrationServiceUrl")}/api/v3/taxonomyList?taxonomyType={encodedRange}";

                        var accessToken = await _registrationServiceTokenService.GetAccessTokenForWebApiAsync();
                        var response = await httpClient.SendRequestWithOptionsAsync(HttpMethod.Get, registrationServiceGetTaxonomyListUrl,
                            null, accessToken, _cancellationToken, _correlationContext.CorrelationContext);

                        if (!response.IsSuccessStatusCode)
                        {
                            _logger.LogError("[Reindexing] Something went wrong while getting taxonomy, response={response}", response);
                            throw new System.Exception("Something went wrong while getting taxonomy");
                        }

                        _logger.LogInformation("[Indexing] Setup metadata mapping: successfully retrieved taxonomies");
                        var result = await response.Content.ReadAsStringAsync().ConfigureAwait(false);

                        taxonomies = JsonConvert.DeserializeObject<List<TaxonomyResultDTO>>(result);
                    }

                    if (taxonomies.Any(entity => entity.HasChild) || metadataProperty.Key == Graph.Metadata.Constants.RDF.Type)
                    {
                        metadataProperty.Properties.AddOrUpdate("taxonomy", taxonomies);
                    }
                }

                foreach (var nestedMetadata in metadataProperty.NestedMetadata)
                {
                    nestedMetadata.Properties = nestedMetadata.Properties
                        .Where(prop => !IsIgnoredMetadataProperty(prop.Key, prop)).ToList();
                }

                return metadataProperty;
            })
                .Select(t => t.Result)
                .Where(prop => !IsIgnoredMetadataProperty(prop.Key, prop))
                .ToDictionary(metaProp => (string)metaProp.Properties[Graph.Metadata.Constants.EnterpriseCore.PidUri], metaProp => metaProp);

            _logger.LogInformation("[Indexing] Created metadata mapping for all entity types");

            return metadataDict;
        }

        private static bool IsIgnoredMetadataProperty(string propertyKey, MetadataProperty metadataProperty, bool inboundProperty = false)
        {
            switch (propertyKey)
            {
                case Graph.Metadata.Constants.Resource.HasPidEntryDraft:
                    return true;
                case Graph.Metadata.Constants.Resource.HasEntryLifecycleStatus:
                    return false;
                case Graph.Metadata.Constants.Resource.MainDistribution:
                    return true;
            }

            if (metadataProperty == null)
            {
                return !inboundProperty;
            }

            var group = metadataProperty.GetMetadataPropertyGroup();

            return group != null && group.Key == Graph.Metadata.Constants.Resource.Groups.InvisibleTechnicalInformation;
        }
    }
}
