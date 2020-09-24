using System;
using System.Collections.Generic;
using System.Linq;
using COLID.IndexingCrawlerService.Repositories.Interface;
using VDS.RDF.Query;
using COLID.Graph.TripleStore.Extensions;
using COLID.Graph.TripleStore.Repositories;
using Microsoft.Extensions.Logging;
using COLID.Graph.Metadata.Repositories;
using COLID.Graph.Metadata.DataModels.Resources;
using COLID.Graph.TripleStore.DataModels.Base;

namespace COLID.IndexingCrawlerService.Repositories.Implementation
{
    public class ResourceRepository : IResourceRepository
    {
        private string InsertingGraph => Graph.Metadata.Constants.MetadataGraphConfiguration.HasResourcesGraph;

        private IEnumerable<string> QueryGraphs => new List<string>() { InsertingGraph };

        private readonly ITripleStoreRepository _tripleStoreRepository;
        private readonly IMetadataGraphConfigurationRepository _metadataGraphConfigurationRepository;

        public ResourceRepository(ITripleStoreRepository tripleStoreRepository, ILogger<ResourceRepository> logger, IMetadataGraphConfigurationRepository metadataGraphConfigurationRepository)
        {
            _tripleStoreRepository = tripleStoreRepository;
            _metadataGraphConfigurationRepository = metadataGraphConfigurationRepository;
        }

        public bool CheckIfResourceExist(Uri pidUri, out string lifeCycleStatus)
        {
            lifeCycleStatus = string.Empty;

            SparqlParameterizedString parameterizedString = new SparqlParameterizedString();
            parameterizedString.CommandText =
                @"Select *
                  @fromResourceNamedGraph
                  @fromMetadataNamedGraph
                  WHERE {
                      ?subject rdf:type [rdfs:subClassOf+ pid3:PID_Concept].
                      ?subject @hasPid @pidUri .
                      FILTER NOT EXISTS { ?subject  @hasPidEntryDraft ?draftSubject }
                      ?subject @lifeCycleStatus ?lifeCycleStatus
                  }";

            parameterizedString.SetPlainLiteral("fromResourceNamedGraph", _metadataGraphConfigurationRepository.GetGraphs(InsertingGraph).JoinAsFromNamedGraphs());
            parameterizedString.SetPlainLiteral("fromMetadataNamedGraph", _metadataGraphConfigurationRepository.GetGraphs(Graph.Metadata.Constants.MetadataGraphConfiguration.HasMetadataGraph).JoinAsFromNamedGraphs());

            parameterizedString.SetUri("hasPid", new Uri(Graph.Metadata.Constants.EnterpriseCore.PidUri));
            parameterizedString.SetUri("pidUri", pidUri);
            parameterizedString.SetUri("lifeCycleStatus", new Uri(Graph.Metadata.Constants.Resource.HasEntryLifecycleStatus));
            parameterizedString.SetUri("hasPidEntryDraft", new Uri(Graph.Metadata.Constants.Resource.HasPidEntryDraft));

            SparqlResultSet result = _tripleStoreRepository.QueryTripleStoreResultSet(parameterizedString);

            if (result.Any())
            {
                lifeCycleStatus = result.FirstOrDefault().GetNodeValuesFromSparqlResult("lifeCycleStatus")?.Value;
                return true;
            }

            return false;
        }

        public IList<Uri> GetAllPublishedPidUris()
        {
            var parameterizedString = new SparqlParameterizedString
            {
                CommandText =
                    @"SELECT DISTINCT ?pidUri
                      @fromResourceNamedGraph
                      @fromMetadataNamedGraph
                      WHERE {
                              ?subject rdf:type  [rdfs:subClassOf* @firstResourceType].
                              Values ?lifecycleStatus { @markedLifecycleStatus @publishedLifecycleStatus }
                              ?subject  @hasLifecycleStatus ?lifecycleStatus.
                              ?subject  @hasPid ?pidUri. 
                            }"
            };

            parameterizedString.SetPlainLiteral("fromResourceNamedGraph", _metadataGraphConfigurationRepository.GetGraphs(InsertingGraph).JoinAsFromNamedGraphs());
            parameterizedString.SetPlainLiteral("fromMetadataNamedGraph", _metadataGraphConfigurationRepository.GetGraphs(Graph.Metadata.Constants.MetadataGraphConfiguration.HasMetadataGraph).JoinAsFromNamedGraphs());
            parameterizedString.SetUri("firstResourceType", new Uri(Graph.Metadata.Constants.Resource.Type.FirstResouceType));
            parameterizedString.SetUri("hasLifecycleStatus", new Uri(Graph.Metadata.Constants.Resource.HasEntryLifecycleStatus));
            parameterizedString.SetUri("hasPid", new Uri(Graph.Metadata.Constants.EnterpriseCore.PidUri));

            parameterizedString.SetUri("markedLifecycleStatus", new Uri(Graph.Metadata.Constants.Resource.ColidEntryLifecycleStatus.MarkedForDeletion));
            parameterizedString.SetUri("publishedLifecycleStatus", new Uri(Graph.Metadata.Constants.Resource.ColidEntryLifecycleStatus.Published));

            var results = _tripleStoreRepository.QueryTripleStoreResultSet(parameterizedString);

            return results.Select(result => new Uri(result.GetNodeValuesFromSparqlResult("pidUri").Value)).ToList();
        }

        public Resource GetPublishedResourceByPidUri(Uri pidUri)
        {
            var parameterizedString = new SparqlParameterizedString
            {
                CommandText =
                    @"PREFIX : <> SELECT DISTINCT ?subject ?object ?predicate ?object_ ?publishedVersion ?inboundPredicate ?inbound
                      @fromResourceNamedGraph
                      WHERE {
                          ?subject @hasPid @pidUri.
                          ?subject rdf:type ?resourceType.
                          Values ?lifecycleStatus { @markedLifecycleStatus @publishedLifecycleStatus }
                          ?subject  @hasLifecycleStatus ?lifecycleStatus.
                          {
                              ?subject ?predicate ?object_.
                              BIND(?subject as ?object).
                              OPTIONAL { ?publishedVersion @hasPidEntryDraft ?subject }
                          } UNION {
                              ?subject (:| !:)+ ?object.
                              ?object ?predicate ?object_.
                              Filter NOT EXISTS { ?draftResource @hasPidEntryDraft ?object}
                          } UNION {
                              ?object ?inboundPredicate ?subject.
                              ?object ?predicate ?object_.
                              BIND(@true as ?inbound).
                              Filter NOT EXISTS { ?draftResource @hasPidEntryDraft ?object}
                        }
                    }"
            };

            parameterizedString.SetPlainLiteral("fromResourceNamedGraph", _metadataGraphConfigurationRepository.GetGraphs(InsertingGraph).JoinAsFromNamedGraphs());
            parameterizedString.SetUri("hasLifecycleStatus", new Uri(Graph.Metadata.Constants.Resource.HasEntryLifecycleStatus));
            parameterizedString.SetUri("hasPidEntryDraft", new Uri(Graph.Metadata.Constants.Resource.HasPidEntryDraft));
            parameterizedString.SetLiteral("true", Graph.Metadata.Constants.Boolean.True);
            parameterizedString.SetUri("hasPid", new Uri(Graph.Metadata.Constants.EnterpriseCore.PidUri));
            parameterizedString.SetUri("pidUri", pidUri);

            parameterizedString.SetUri("markedLifecycleStatus", new Uri(Graph.Metadata.Constants.Resource.ColidEntryLifecycleStatus.MarkedForDeletion));
            parameterizedString.SetUri("publishedLifecycleStatus", new Uri(Graph.Metadata.Constants.Resource.ColidEntryLifecycleStatus.Published));

            var results = _tripleStoreRepository.QueryTripleStoreResultSet(parameterizedString);

            return results.IsEmpty ? null : TransformQueryResults(results, pidUri.ToString()).FirstOrDefault();
        }

        private IList<Resource> TransformQueryResults(SparqlResultSet results, string id = "")
        {
            if (results.IsEmpty)
            {
                return new List<Resource>();
            }

            var groupedResults = results.GroupBy(result => result.GetNodeValuesFromSparqlResult("subject").Value);

            var counter = 0;
            var inboundCounter = 0;

            return groupedResults.Select(result => CreateResourceFromGroupedResult(result, counter, inboundCounter)).ToList();
        }

        private Resource CreateResourceFromGroupedResult(IGrouping<string, SparqlResult> result, int counter, int inboundCounter)
        {
            var id = result.Key;

            var newEntity = new Resource
            {
                Id = id,
                PublishedVersion = result.FirstOrDefault().GetNodeValuesFromSparqlResult("publishedVersion").Value,
                Properties = GetEntityPropertiesFromSparqlResultByList(result, id, counter),
                InboundProperties = GetInboundEntityPropertiesFromSparqlResultByList(result, inboundCounter)
            };

            newEntity.Versions = GetAllVersionsOfResourceByPidUri(newEntity.PidUri);

            return newEntity;
        }

        private IDictionary<string, List<dynamic>> GetEntityPropertiesFromSparqlResultByList(IGrouping<string, SparqlResult> sparqlResults, string id, int counter)
        {
            // sparqlResults are a list of all properties of one resource inkl. subentites
            counter++;
            // filtered for actual entity
            var filteredResults = sparqlResults.Where(t => t.GetNodeValuesFromSparqlResult("object").Value == id);

            var groupedFilteredResults = filteredResults.GroupBy(t => t.GetNodeValuesFromSparqlResult("predicate").Value);

            return groupedFilteredResults.ToDictionary(
                res => res.Key,
                res =>
                {
                    return res.Select(subRes =>
                    {
                        var key = res.Key;
                        var valueProperty = subRes.GetNodeValuesFromSparqlResult("object_");
                        var valuePropertyPidUri = subRes.GetNodeValuesFromSparqlResult("objectPidUri");

                        dynamic value = null;
                        if (valueProperty.Type == Graph.Metadata.Constants.Shacl.NodeKinds.IRI && sparqlResults.Any(t => t.GetNodeValuesFromSparqlResult("object").Value == valueProperty.Value) && counter <= 4)
                        {
                            value = new Entity()
                            {
                                Id = valueProperty.Value,
                                Properties = GetEntityPropertiesFromSparqlResultByList(sparqlResults, valueProperty.Value, counter)
                            };
                        }
                        else
                        {
                            value = string.IsNullOrWhiteSpace(valuePropertyPidUri.Value) ? valueProperty.Value : valuePropertyPidUri.Value;
                        }

                        return value;
                    }).ToList(); ;
                }); ;
        }

        private IDictionary<string, List<dynamic>> GetInboundEntityPropertiesFromSparqlResultByList(IGrouping<string, SparqlResult> sparqlResults, int counter)
        {
            // sparqlResults are a list of all properties of one resource inkl. subentites
            counter++;
            // filtered for actual entity and no laterVersion
            var filteredResults = sparqlResults.Where(t => t.GetNodeValuesFromSparqlResult("inbound").Value == Graph.Metadata.Constants.Boolean.True && t.GetNodeValuesFromSparqlResult("inboundPredicate").Value != Graph.Metadata.Constants.Resource.HasLaterVersion);

            var groupedResults = filteredResults.GroupBy(t => t.GetNodeValuesFromSparqlResult("inboundPredicate").Value);

            return groupedResults.ToDictionary(
                res => res.FirstOrDefault().GetNodeValuesFromSparqlResult("inboundPredicate").Value,
                res =>
                {
                    var subGroupedResults = res.GroupBy(t => t.GetNodeValuesFromSparqlResult("object").Value);

                    return subGroupedResults.Select(subRes =>
                    {
                        var key = subRes.FirstOrDefault().GetNodeValuesFromSparqlResult("inboundPredicate").Value;
                        var valueProperty = subRes.Key;

                        var value = new Entity()
                        {
                            Id = valueProperty,
                            Properties = GetEntityPropertiesFromSparqlResultByList(subRes, valueProperty, counter)
                        };

                        return (dynamic)value;
                    }).ToList();
                });
        }

        public IList<VersionOverviewCTO> GetAllVersionsOfResourceByPidUri(Uri pidUri)
        {
            if (pidUri == null)
            {
                return new List<VersionOverviewCTO>();
                //throw new ArgumentNullException(nameof(pidUri));
            }

            var parameterizedString = new SparqlParameterizedString
            {
                CommandText = @"SELECT DISTINCT ?resource ?pidUri ?version ?baseUri ?entryLifecycleStatus ?publishedResource
                  @fromResourceNamedGraph
                  WHERE {
                  ?subject @hasPid @hasPidUri.
                  Filter NOT EXISTS{?_subject @hasPidEntryDraft ?subject}
                      {
                      ?resource @hasLaterVersion* ?subject.
                      ?resource pid3:hasVersion ?version .
                      ?resource @hasPid ?pidUri .
                      ?resource @hasEntryLifecycleStatus ?entryLifecycleStatus.
                      OPTIONAL { ?resource @hasBaseUri ?baseUri }.
                      OPTIONAL { ?publishedResource @hasPidEntryDraft ?resource }.
                  } UNION {
                      ?subject @hasLaterVersion* ?resource.
                      ?resource pid3:hasVersion ?version .
                      ?resource @hasPid ?pidUri .
                      ?resource @hasEntryLifecycleStatus ?entryLifecycleStatus.
                      OPTIONAL { ?resource @hasBaseUri ?baseUri }.
                      OPTIONAL { ?publishedResource @hasPidEntryDraft ?resource }.
                  }
                  Filter NOT EXISTS { ?draftResource  @hasPidEntryDraft ?resource}
                  }
                  ORDER BY ASC(?version)"
            };

            // Select all resources with their PID and target Url, which are of type resource and published

            parameterizedString.SetPlainLiteral("fromResourceNamedGraph", _metadataGraphConfigurationRepository.GetGraphs(InsertingGraph).JoinAsFromNamedGraphs());
            parameterizedString.SetUri("hasPidUri", pidUri);
            parameterizedString.SetUri("hasPid", new Uri(Graph.Metadata.Constants.EnterpriseCore.PidUri));
            parameterizedString.SetUri("hasBaseUri", new Uri(Graph.Metadata.Constants.Resource.BaseUri));
            parameterizedString.SetUri("hasPidEntryDraft", new Uri(Graph.Metadata.Constants.Resource.HasPidEntryDraft));
            parameterizedString.SetUri("hasEntryLifecycleStatus", new Uri(Graph.Metadata.Constants.Resource.HasEntryLifecycleStatus));
            parameterizedString.SetUri("hasLaterVersion", new Uri(Graph.Metadata.Constants.Resource.HasLaterVersion));

            var results = _tripleStoreRepository.QueryTripleStoreResultSet(parameterizedString);

            if (results.IsEmpty)
            {
                return new List<VersionOverviewCTO>();
            }

            var resourceVersions = results.Select(result => new VersionOverviewCTO()
            {
                Id = result.GetNodeValuesFromSparqlResult("resource").Value,
                Version = result.GetNodeValuesFromSparqlResult("version").Value,
                PidUri = result.GetNodeValuesFromSparqlResult("pidUri").Value,
                BaseUri = result.GetNodeValuesFromSparqlResult("baseUri").Value,
                LifecycleStatus = result.GetNodeValuesFromSparqlResult("entryLifecycleStatus").Value,
                PublishedVersion = result.GetNodeValuesFromSparqlResult("publishedResource")?.Value
            }).ToList();

            return resourceVersions;
        }
    }
}
