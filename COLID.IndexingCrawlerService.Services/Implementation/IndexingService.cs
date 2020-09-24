using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using System.Web;
using COLID.Cache.Services;
using COLID.Common.Extensions;
using COLID.Graph.HashGenerator.Services;
using COLID.Graph.Metadata.DataModels.MessageQueuing;
using COLID.Graph.Metadata.DataModels.Metadata;
using COLID.Graph.Metadata.DataModels.Resources;
using COLID.Graph.Metadata.Extensions;
using COLID.Graph.Metadata.Services;
using COLID.Graph.TripleStore.DataModels.Base;
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
        private readonly IConfiguration _configuration;
        private readonly ColidMessageQueueOptions _mqOptions;
        private readonly JsonSerializerSettings _serializerSettings;
        private readonly int LevelLinking = 2;

        private const string ResourceId = "resourceId";
        private const string InternalResourceId = "internalResourceId";
        private const string ResourceHash = "resourceHash";

        public IDictionary<string, Action<string>> OnTopicReceivers => new Dictionary<string, Action<string>>() {
            {_mqOptions.Topics["TopicResourcePublishedPidUriIndexing"], SendPublishedResourceIndex },
            {_mqOptions.Topics["TopicResourcePublishedPidUri"], SendPublishedResource },
            {_mqOptions.Topics["TopicResourceDeletedPidUri"], SendResourceDeleted }
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
                var response = await httpClient.SendRequestWithBearerTokenAsync(HttpMethod.Post, searchServiceIndexCreateUrl,
                    metadataMapping, accessToken, _cancellationToken);

                if (!response.IsSuccessStatusCode)
                {
                    _logger.LogError("[Reindexing] Something went wrong while starting reindexing\nResponse Content={responseContent}", response.Content);
                    throw new System.Exception("Something went wrong while starting reindexing");
                }

                SendAllResources();

                return;
            }
        }

        /// <summary>
        /// Get the pid uri of all published entries and send it to mq
        /// </summary>
        private void SendAllResources()
        {
            _logger.LogInformation("[Reindexing] Send all resources");

            _logger.LogInformation("[Reindexing] Getting all resource pid uris");
            var pidUris = _resourceService.GetAllPublishedPidUris();

            _logger.LogInformation("[Reindexing] Sending {pidUrisCount} resources", pidUris.Count);

            foreach (var pidUri in pidUris)
            {
                try
                {
                    _logger.LogInformation("[Reindexing] Sending PID URI to topic\nPidUri={PidUri}\nMqTopic={MQTopic}", pidUri, "TopicResourcePublishedPidUriIndexing");
                    PublishMessage(_mqOptions.Topics["TopicResourcePublishedPidUriIndexing"], pidUri.ToString(), new BasicProperty() { Priority = 0 });
                }
                catch (System.Exception ex)
                {
                    _logger.LogError(ex, "[Reindexing] FAILED - Sending PID URI to topic\nPidUri={PidUri}\nMqTopic={MQTopic}", pidUri, "TopicResourcePublishedPidUriIndexing");
                }
            }
        }

        /// <summary>
        /// Send the published resource and its links to the mq
        /// </summary>
        /// <param name="pidUri">PID URI of published resource</param>
        public void SendPublishedResource(string pidUriString)
        {
            byte priority = 0;

            if (!Uri.TryCreate(pidUriString, UriKind.Absolute, out Uri pidUri))
            {
                _logger.LogError("[Indexing] Sending the published resource failed - invalid url.\nPidUri={PidUri}", pidUriString);
                return;
            }

            var resource = _resourceService.GetPublishedResourceByPidUri(pidUri);

            if (resource == null)
            {
                _logger.LogError("[Indexing] Sending the published resource failed - no resource found.\nPidUri={PidUri}", pidUriString);
                return;
            }

            string resourceType = resource.Properties.GetValueOrNull(Graph.Metadata.Constants.RDF.Type, true);

            var metadataProperties = _metadataService.GetMetadataForEntityType(resourceType);

            var linkedPidUris = resource.GetLinks(metadataProperties);

            SendPublishedResource(resource, metadataProperties, priority);

            foreach (var linkedPidUri in linkedPidUris)
            {
                // To avoid that a change in the repo causes the main resource to be published more than once
                if (linkedPidUri != pidUriString)
                {
                    var linkedResource = _resourceService.GetPublishedResourceByPidUri(new Uri(linkedPidUri));

                    SendPublishedResource(linkedResource, priority);
                }
            }
        }

        /// <summary>
        /// Send the published resource to the mq
        /// </summary>
        /// <param name="pidUri">Pid uri of published resource</param>
        public void SendPublishedResourceIndex(string pidUri)
        {
            if (string.IsNullOrWhiteSpace(pidUri)) return;

            var resource = _resourceService.GetPublishedResourceByPidUri(new Uri(pidUri));

            SendPublishedResource(resource, 9);
        }

        /// <summary>
        /// Send the published resource to the mq
        /// </summary>
        /// <param name="resource">Resource to be send</param>
        /// <param name="priority">Defines the priority of the entry for the mq. High the more important</param>
        public void SendPublishedResource(Resource resource, byte priority)
        {
            if (resource == null) return;

            string resourceType = resource.Properties.GetValueOrNull(Graph.Metadata.Constants.RDF.Type, true);

            var metadata = _metadataService.GetMetadataForEntityType(resourceType);

            SendPublishedResource(resource, metadata, priority);
        }

        /// <summary>
        /// Send the published resource to the mq
        /// </summary>
        /// <param name="resource">Resource to be send</param>
        /// <param name="metadataProperties">Related metadata</param>
        public void SendPublishedResource(Resource resource, IList<MetadataProperty> metadataProperties, byte priority)
        {
            if (resource == null || !metadataProperties.Any())
            {
                _logger.LogInformation("[Indexing] Send published resource. No resource or no metatdata\nPidUri={PidUri}\nMqTopic={MQTopic}", resource?.PidUri, "TopicResourcePublished");
                return;
            }

            try
            {
                var message = JsonConvert.SerializeObject(GenerateMqMessage(resource, metadataProperties), _serializerSettings);

                _logger.LogInformation("[Indexing] Publish mq message for resource.\nPidUri={PidUri}\nMqTopic={MQTopic}", resource?.PidUri, "TopicResourcePublished");
                PublishMessage(_mqOptions.Topics["TopicResourcePublished"], message, new BasicProperty() { Priority = priority });
            }
            catch (System.Exception ex)
            {
                _logger.LogError(ex, "[Indexing] Something went wrong while sending the resource.\nPidUri={PidUri}\nMqTopic={MQTopic}", resource?.PidUri, "TopicResourcePublished");
            }
        }

        /// <summary>
        /// Delete resource with given identifier in index
        /// </summary>
        /// <param name="pidUri">Identifier of resource to be deleted</param>
        private void SendResourceDeleted(string pidUri)
        {
            if (string.IsNullOrWhiteSpace(pidUri)) return;

            var mqMessageDict = new Dictionary<string, MessageQueuePropertyDTO>();

            var resourceIdMqProperty = GenerateResourceIdMessageQueueProperty(new Uri(pidUri));
            mqMessageDict.Add(ResourceId, resourceIdMqProperty);

            try
            {
                var message = JsonConvert.SerializeObject(mqMessageDict, _serializerSettings);
                PublishMessage(_mqOptions.Topics["TopicResourceDeleted"], message, new BasicProperty() { Priority = 0 });
            }
            catch (System.Exception ex)
            {
                _logger.LogError(ex, "[Indexing] FAILED Deleting resource.\nPidUri={PidUri}\nMqTopic={MQTopic}", pidUri, "TopicResourceDeleted");
            }
        }

        private IDictionary<string, MessageQueuePropertyDTO> GenerateMqMessage(Resource resource, IList<MetadataProperty> metadataProperties)
        {
            _logger.LogInformation("[Indexing] Generating mq message for resource\nPidUri={PidUri}", resource.PidUri);

            var mqMessageDict = GenerateMqMessage(resource as Entity, metadataProperties);

            var pointsAtMqProperty = GeneratePointsAtMessageQueueProperty(resource);

            if (pointsAtMqProperty != null)
            {
                mqMessageDict.Add(Graph.Metadata.Constants.Resource.PointAt, pointsAtMqProperty);
            }

            AddAdditionalMqProperties(resource, mqMessageDict);

            _logger.LogInformation("[Indexing] Generated mq message for resource\nPidUri={PidUri}", resource.PidUri);

            return mqMessageDict;
        }

        /// <summary>
        /// Adds further properties to the document, which are relevant for the index.
        /// </summary>
        /// <param name="resource">resource to add additional properties for</param>
        /// <param name="mqMessageDict">Dictionary with all normal resource properties</param>
        private void AddAdditionalMqProperties(Resource resource, IDictionary<string, MessageQueuePropertyDTO> mqMessageDict)
        {
            var versionMqProperty = GenerateVersionMqProperty(resource);
            mqMessageDict.Add(Graph.Metadata.Constants.Resource.HasVersions, versionMqProperty);

            var resourceIdMqProperty = GenerateResourceIdMessageQueueProperty(resource.PidUri);
            mqMessageDict.Add(ResourceId, resourceIdMqProperty);

            var internalResourceIdMqProperty = GenerateInternalResourceIdMessageQueueProperty(resource);
            mqMessageDict.Add(InternalResourceId, internalResourceIdMqProperty);

            var internalResourceHashMqProperty = GenerateResourceHashMessageQueueProperty(resource);
            mqMessageDict.Add(ResourceHash, internalResourceHashMqProperty);
        }

        /// <summary>
        /// Generates an mq property for the versions. all previous versions are stored as inbound values and all older versions are stored as outbound values.
        /// </summary>
        /// <param name="resource">The resource from the version chain that is to be updated</param>
        /// <returns>Returns an mq property for the versions chain</returns>
        private static MessageQueuePropertyDTO GenerateVersionMqProperty(Resource resource)
        {
            string actualVersion = resource.Properties.GetValueOrNull(Graph.Metadata.Constants.Resource.HasVersion, true);

            var messageQueueProperty = new MessageQueuePropertyDTO();

            foreach (var version in resource.Versions)
            {
                if (!CheckVersionHasPublishedVersion(version))
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
        private static MessageQueuePropertyDTO GeneratePointsAtMessageQueueProperty(Resource resource)
        {
            Entity mainDistribution = resource.Properties.GetValueOrNull(Graph.Metadata.Constants.Resource.MainDistribution, true);
            if (mainDistribution != null)
            {
                var propertyDTO = new MessageQueueDirectionPropertyDTO(null, mainDistribution.Id, Graph.Metadata.Constants.Resource.PointAt);
                return new MessageQueuePropertyDTO() { Outbound = new List<MessageQueueDirectionPropertyDTO>() { propertyDTO } };
            }

            return null;
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
        private static MessageQueuePropertyDTO GenerateInternalResourceIdMessageQueueProperty(Resource resource)
        {
            return new MessageQueuePropertyDTO() { Outbound = new List<MessageQueueDirectionPropertyDTO>() { new MessageQueueDirectionPropertyDTO(null, resource.Id) } };
        }

        /// <summary>
        /// Generates a mq property for hashed resources.
        /// </summary>
        /// <param name="resource">The resource used to generate the hash</param>
        /// <returns>Returns an mq property</returns>
        private MessageQueuePropertyDTO GenerateResourceHashMessageQueueProperty(Resource resource)
        {
            var sha512Hash = _hasher.Hash(resource);
            return new MessageQueuePropertyDTO() { Outbound = new List<MessageQueueDirectionPropertyDTO> { new MessageQueueDirectionPropertyDTO(sha512Hash, null) } };
        }

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

        private IDictionary<string, MessageQueuePropertyDTO> GenerateMqMessage(Entity entity, IList<MetadataProperty> metadataProperties)
        {
            var linkingLevelIndexer = 0;
            var mqMessageDict = GenerateMqMessage(entity, metadataProperties, linkingLevelIndexer);

            var inboundLinkingLevelIndexer = 0;
            var mqMessageDictInbound = GenerateMqMessage(entity, metadataProperties, inboundLinkingLevelIndexer, true);

            CombineInboundAndOutboundProperties(mqMessageDict, mqMessageDictInbound);

            return mqMessageDict;
        }

        private IDictionary<string, MessageQueuePropertyDTO> GenerateMqMessage(Entity entity, IList<MetadataProperty> metadataProperties, int linkingLevelIndexer, bool inbound = false)
        {
            var mqMessageDict = new Dictionary<string, MessageQueuePropertyDTO>();

            IDictionary<string, List<dynamic>> resourceProperties;
            if (inbound)
            {
                resourceProperties = entity.InboundProperties;
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
                if (IsIgnoredMetadataProperty(metadataProperty, inbound) && Graph.Metadata.Constants.Resource.MainDistribution != metadataProperty?.Key)
                {
                    continue;
                }

                foreach (var property in propertyItem.Value)
                {
                    var newLinkingLevelIndexer = linkingLevelIndexer;

                    // in some cases, the inbound type may not be the same as the current entity type, missing metadata must be loaded
                    if (metadataProperty == null && inbound && property is Entity)
                    {
                        var inboundEntity = property as Entity;
                        var inboundEntityType = inboundEntity.Properties.GetValueOrNull(Graph.Metadata.Constants.RDF.Type, true);

                        IList<MetadataProperty> newMetadataProperties = _metadataService.GetMetadataForEntityType(inboundEntityType);
                        metadataProperty = newMetadataProperties?.FirstOrDefault(metaProp => metaProp.Properties.GetValueOrNull(Graph.Metadata.Constants.EnterpriseCore.PidUri, true) == propertyItem.Key);
                    }

                    if (metadataProperty != null)
                    {
                        var mqProperty = GenerateMqDirectionProperty(propertyItem.Key, property, metadataProperty, newLinkingLevelIndexer);

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

        private MessageQueueDirectionPropertyDTO GenerateMqDirectionProperty(string propertyKey, dynamic propertyValue, MetadataProperty metadataProperty, int linkingLevelIndexer)
        {
            var metadataGroup = metadataProperty.GetMetadataPropertyGroup();

            if (propertyValue is Entity)
            {
                linkingLevelIndexer++;

                var nestedEntity = propertyValue as Entity;

                var mqProperty = new MessageQueueDirectionPropertyDTO(nestedEntity.Id, nestedEntity.Id, propertyKey);

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

                if (!string.IsNullOrWhiteSpace(entryLifeCycleStatus) &&
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
                    mqProperty.Value = GenerateMqMessage(nestedEntity, metadataForNestedEntityType.Properties, linkingLevelIndexer);
                }

                return mqProperty;
            }
            else if (metadataGroup?.Key == Graph.Metadata.Constants.Resource.Groups.LinkTypes)
            {
                return null;
            }
            else if (metadataProperty.IsControlledVocabulary(out var range) || propertyKey == Graph.Metadata.Constants.RDF.Type)
            {
                var id = propertyValue as string;

                var entity = _entityService.GetEntity(id);

                if (entity == null)
                {
                    _logger.LogWarning($"Unable to map CV for property with key={propertyKey} and value={propertyValue}.");
                    return null;
                }

                return new MessageQueueDirectionPropertyDTO(entity.Name, id);
            }
            else
            {
                return new MessageQueueDirectionPropertyDTO(propertyValue as string, null);
            }
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
                        var response = await httpClient.SendRequestWithBearerTokenAsync(HttpMethod.Get, registrationServiceGetTaxonomyListUrl,
                            null, accessToken, _cancellationToken);

                        if (!response.IsSuccessStatusCode)
                        {
                            _logger.LogError("[Reindexing] Something went wrong while getting taxonomy\nresponse={response}", response);
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
                        .Where(prop => !IsIgnoredMetadataProperty(prop)).ToList();
                }

                return metadataProperty;
            })
                .Select(t => t.Result)
                .Where(prop => !IsIgnoredMetadataProperty(prop))
                .ToDictionary(metaProp => (string)metaProp.Properties[Graph.Metadata.Constants.EnterpriseCore.PidUri], metaProp => metaProp);

            _logger.LogInformation("[Indexing] Created metadata mapping for all entity types");

            return metadataDict;
        }

        private static bool IsIgnoredMetadataProperty(MetadataProperty metadataProperty, bool inboundProperty = false)
        {
            if (metadataProperty == null || Graph.Metadata.Constants.Resource.MainDistribution == metadataProperty.Key)
            {
                return !inboundProperty;
            }

            var group = metadataProperty.GetMetadataPropertyGroup();

            return group != null && group.Key == Graph.Metadata.Constants.Resource.Groups.InvisibleTechnicalInformation;
        }
    }
}
