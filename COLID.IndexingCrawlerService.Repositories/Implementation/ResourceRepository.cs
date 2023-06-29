using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using COLID.Exception.Models.Business;
using COLID.IndexingCrawlerService.Repositories.Interfaces;
using VDS.RDF.Query;
using COLID.Graph.TripleStore.Extensions;
using COLID.Graph.TripleStore.Repositories;
using Microsoft.Extensions.Logging;
using COLID.Graph.Metadata.Repositories;
using COLID.Graph.Metadata.DataModels.Resources;
using COLID.Graph.TripleStore.DataModels.Base;
using COLID.RegistrationService.Common.DataModel.Resources;
using COLID.Common.Extensions;

namespace COLID.IndexingCrawlerService.Repositories.Implementation
{
    public class ResourceRepository : IResourceRepository
    {
        private static string InsertingGraph => Graph.Metadata.Constants.MetadataGraphConfiguration.HasResourcesGraph;
        private static string InsertingDraftGraph => Graph.Metadata.Constants.MetadataGraphConfiguration.HasResourcesDraftGraph;

        private static IEnumerable<string> QueryGraphs => new List<string>() { InsertingGraph, InsertingDraftGraph };

        private readonly ITripleStoreRepository _tripleStoreRepository;
        private readonly IMetadataGraphConfigurationRepository _metadataGraphConfigurationRepository;

        public ResourceRepository(ITripleStoreRepository tripleStoreRepository, ILogger<ResourceRepository> logger, IMetadataGraphConfigurationRepository metadataGraphConfigurationRepository)
        {
            _tripleStoreRepository = tripleStoreRepository;
            _metadataGraphConfigurationRepository = metadataGraphConfigurationRepository;
        }

        public bool CheckIfResourceExist(Uri pidUri, IList<string> resourceTypes, Uri namedGraph)
        {

            SparqlParameterizedString parameterizedString = new SparqlParameterizedString();
            parameterizedString.CommandText =
                @"Select DISTINCT ?subject
                 FROM @fromResourceNamedGraph
                  WHERE {
                      VALUES ?type { @resourceTypes }.
                      ?subject @hasPid @pidUri .
                      FILTER NOT EXISTS { ?subject  @hasPidEntryDraft ?draftSubject }
                      ?subject @lifeCycleStatus ?lifeCycleStatus
                  }";

            //parameterizedString.SetPlainLiteral("fromResourceNamedGraph", _metadataGraphConfigurationRepository.GetGraphs(QueryGraphs).JoinAsFromNamedGraphs());
            parameterizedString.SetUri("fromResourceNamedGraph", namedGraph);
            //parameterizedString.SetPlainLiteral("fromMetadataNamedGraph", _metadataGraphConfigurationRepository.GetGraphs(Graph.Metadata.Constants.MetadataGraphConfiguration.HasMetadataGraph).JoinAsFromNamedGraphs());
            parameterizedString.SetPlainLiteral("resourceTypes", resourceTypes.JoinAsValuesList());

            parameterizedString.SetUri("hasPid", new Uri(Graph.Metadata.Constants.EnterpriseCore.PidUri));
            parameterizedString.SetUri("pidUri", pidUri);
            parameterizedString.SetUri("lifeCycleStatus", new Uri(Graph.Metadata.Constants.Resource.HasEntryLifecycleStatus));
            parameterizedString.SetUri("hasPidEntryDraft", new Uri(Graph.Metadata.Constants.Resource.HasPidEntryDraft));

            SparqlResultSet result = _tripleStoreRepository.QueryTripleStoreResultSet(parameterizedString);
            return result.Any();
            //if (result.Any())
            //{
            //    foreach (var item in result)
            //    {
            //        lifeCycleStatus = result.FirstOrDefault().GetNodeValuesFromSparqlResult("lifeCycleStatus")?.Value;

            //    }
            //    return true;
            //}

            //return false;
        }

        public IList<Uri> GetAllPidUris()
        {
            var parameterizedString = new SparqlParameterizedString
            {
                CommandText =
                    @"SELECT DISTINCT ?pidUri
                      @fromResourceNamedGraph
                      @fromMetadataNamedGraph
                      WHERE {
                              ?subject rdf:type  [rdfs:subClassOf* @firstResourceType].
                              ?subject  @hasPid ?pidUri. 
                              FILTER NOT EXISTS { ?subject  @hasPidEntryDraft ?draftResource. }.
                            }"
            };

            parameterizedString.SetPlainLiteral("fromResourceNamedGraph", _metadataGraphConfigurationRepository.GetGraphs(QueryGraphs).JoinAsFromNamedGraphs());
            parameterizedString.SetPlainLiteral("fromMetadataNamedGraph", _metadataGraphConfigurationRepository.GetGraphs(Graph.Metadata.Constants.MetadataGraphConfiguration.HasMetadataGraph).JoinAsFromNamedGraphs());
            parameterizedString.SetUri("firstResourceType", new Uri(Graph.Metadata.Constants.Resource.Type.FirstResouceType));
            parameterizedString.SetUri("hasPidEntryDraft", new Uri(Graph.Metadata.Constants.Resource.HasPidEntryDraft));
            parameterizedString.SetUri("hasPid", new Uri(Graph.Metadata.Constants.EnterpriseCore.PidUri));

            var results = _tripleStoreRepository.QueryTripleStoreResultSet(parameterizedString);

            return results.Select(result => new Uri(result.GetNodeValuesFromSparqlResult("pidUri").Value)).ToList();
        }

        public ResourcesCTO GetResourcesByPidUri(Uri pidUri, IList<string> resourceTypes, Dictionary<Uri, bool> namedGraphs)
        {
            ISet<Uri> namedGraph = namedGraphs.Where(x => x.Value).Select(x => x.Key).ToHashSet();
            var graphName = (namedGraph.Count > 1) ? "" : namedGraph.FirstOrDefault().ToString();
            
            SparqlParameterizedString parameterizedString = new SparqlParameterizedString
            {
                CommandText =
               @"SELECT DISTINCT ?subject ?object ?predicate ?object_ ?publishedVersion ?objectPidUri ?inbound ?inboundPredicate ?inboundPidUri
                  FROM @resourceNamedGraph 
                  WHERE { {
                     ?subject @hasPid @pidUri.
                     BIND(?subject as ?object).
                     ?subject ?predicate ?object_.
                     OPTIONAL { ?publishedVersion @hasPidEntryDraft ?subject }
                     OPTIONAL { ?object_ @hasPid ?objectPidUri. }
                  } UNION {
                    ?subject @hasPid @pidUri.
                    ?subject (rdf:| !rdf:)+ ?object.
                    ?object rdf:type ?objectType.
                    FILTER (?objectType NOT IN ( @resourceTypes ) )
                    ?object ?predicate ?object_.
                    }
                    UNION {
                    ?subject @hasPid @pidUri.
                    ?object ?inboundPredicate ?subject.
                    ?object @hasPid ?inboundPidUri.
                    BIND(@true as ?inbound).
                    Filter NOT EXISTS { ?draftResource @hasPidEntryDraft ?object}
                    }
                    }"
            };

            //parameterizedString.SetPlainLiteral("fromResourceNamedGraph", _metadataGraphConfigurationRepository.GetGraphs(QueryGraphs).JoinAsFromNamedGraphs());
            parameterizedString.SetUri("hasPid", new Uri(Graph.Metadata.Constants.EnterpriseCore.PidUri));
            parameterizedString.SetUri("pidUri", pidUri);
            parameterizedString.SetUri("hasPidEntryDraft", new Uri(Graph.Metadata.Constants.Resource.HasPidEntryDraft));
            parameterizedString.SetPlainLiteral("resourceTypes", resourceTypes.JoinAsGraphsList());
            parameterizedString.SetLiteral("true", Graph.Metadata.Constants.Boolean.True);

            /*
            var resources = BuildResourceFromQuery(parameterizedString, pidUri);

            var resourceDraft = resources.FirstOrDefault(r =>
                r.Properties.GetValueOrNull(Graph.Metadata.Constants.Resource.HasEntryLifecycleStatus, true) == Graph.Metadata.Constants.Resource.ColidEntryLifecycleStatus.Draft); // firstResource
            var resourcePublished = resources.FirstOrDefault(r =>
            {
                var lifecycleStatus =
                    r.Properties.GetValueOrNull(Graph.Metadata.Constants.Resource.HasEntryLifecycleStatus, true);

                return lifecycleStatus == Graph.Metadata.Constants.Resource.ColidEntryLifecycleStatus.Published ||
                       lifecycleStatus == Graph.Metadata.Constants.Resource.ColidEntryLifecycleStatus.MarkedForDeletion;
            });

            IList<VersionOverviewCTO> versions = new List<VersionOverviewCTO>();
            if (resourceDraft != null || resourcePublished != null)
            {
                versions = resourceDraft == null ? resourcePublished.Versions : resourceDraft.Versions;
            }

            ResourcesCTO resourcesCTO = new ResourcesCTO(resourceDraft, resourcePublished, versions);

            return resourcesCTO;
            */
            Resource resourcePublished = new Resource();
            Resource resourceDraft = new Resource();

            if (graphName.IsNullOrEmpty())
            {
                var draftUri = namedGraphs.Where(x => x.Key.ToString().ToUpper().Contains("DRAFT", StringComparison.Ordinal)).Select(x => x.Key).FirstOrDefault();
                var publishedUri = namedGraphs.Where(x => !x.Key.ToString().ToUpper().Contains("DRAFT", StringComparison.Ordinal)).Select(x => x.Key).FirstOrDefault();

                parameterizedString.SetUri("resourceNamedGraph", publishedUri); // Sowohl aus draft als auch aus publish

                namedGraphs.Clear();
                namedGraphs.Add(publishedUri, true);
                namedGraphs.Add(draftUri, false);
                resourcePublished = BuildResourceFromQuery(parameterizedString, pidUri, namedGraphs).FirstOrDefault(r =>
                {
                    var lifecycleStatus =
                        r.Properties.GetValueOrNull(Graph.Metadata.Constants.Resource.HasEntryLifecycleStatus, true);

                    return lifecycleStatus == Graph.Metadata.Constants.Resource.ColidEntryLifecycleStatus.Published ||
                           lifecycleStatus == Graph.Metadata.Constants.Resource.ColidEntryLifecycleStatus.MarkedForDeletion;

                });  // PREVIOUSVERSIONS AUS DER PUBLIC RESOURCE WERDEN RAUSGEHOLT -> TODO : LINKS SEPERAT RAUSHOLEN

                parameterizedString.SetUri("resourceNamedGraph", draftUri);
                namedGraphs.Clear();
                namedGraphs.Add(publishedUri, false);
                namedGraphs.Add(draftUri, true);
                resourceDraft = BuildResourceFromQuery(parameterizedString, pidUri, namedGraphs).FirstOrDefault(); // PREVIOUSVERSIONS AUS DER PUBLIC RESOURCE WERDEN RAUSGEHOLT -> TODO : LINKS SEPERAT RAUSHOLEN
            }
            else if (graphName.ToUpper().Contains("DRAFT", StringComparison.Ordinal))
            {
                parameterizedString.SetUri("resourceNamedGraph", new Uri(graphName));
                resourceDraft = BuildResourceFromQuery(parameterizedString, pidUri, namedGraphs).FirstOrDefault(); // PREVIOUSVERSIONS AUS DER PUBLIC RESOURCE WERDEN RAUSGEHOLT -> TODO : LINKS SEPERAT RAUSHOLEN
                resourcePublished = null;
            }
            else
            {
                parameterizedString.SetUri("resourceNamedGraph", new Uri(graphName));
                resourcePublished = BuildResourceFromQuery(parameterizedString, pidUri, namedGraphs).FirstOrDefault(

                /*    r =>
                {
                    var lifecycleStatus =
                        r.Properties.GetValueOrNull(Graph.Metadata.Constants.Resource.HasEntryLifecycleStatus, true);

                    return lifecycleStatus == Graph.Metadata.Constants.Resource.ColidEntryLifecycleStatus.Published ||
                           lifecycleStatus == Graph.Metadata.Constants.Resource.ColidEntryLifecycleStatus.MarkedForDeletion;
                }*/);
                resourceDraft = null;
            }

            IList<VersionOverviewCTO> versions = new List<VersionOverviewCTO>();

            if (resourceDraft != null || resourcePublished != null)
            {
                versions = resourceDraft == null ? resourcePublished.Versions : resourceDraft.Versions;
            }

            ResourcesCTO resourcesCTO = new ResourcesCTO(resourceDraft, resourcePublished, versions);
            return resourcesCTO;
        }

        private IList<Resource> BuildResourceFromQuery(SparqlParameterizedString parameterizedString, Uri pidUri, Dictionary<Uri, bool> namedGraphs)
        {

            ISet<Uri> namedGraph = namedGraphs.Where(x => x.Value).Select(x => x.Key).ToHashSet();
            ISet<Uri> allgraphs = namedGraphs.Select(x => x.Key).ToHashSet();

            using var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(10));

            var resultsTask = Task.Factory.StartNew(() => _tripleStoreRepository.QueryTripleStoreResultSet(parameterizedString), cancellationTokenSource.Token);
            var versionsTask = Task.Factory.StartNew(() => GetAllVersionsOfResourceByPidUri(pidUri, allgraphs), cancellationTokenSource.Token);

            WaitAllTasks(cancellationTokenSource, resultsTask, versionsTask);
            SparqlResultSet results = resultsTask.Result;
            IList<VersionOverviewCTO> versions = versionsTask.Result;

            if (!results.Any())
            {
                throw new EntityNotFoundException("No resource found for the given endpoint PID URI.", pidUri.ToString());
            }

            var entities = TransformQueryResults(results, allgraphs ,pidUri?.ToString());

            foreach (var entity in entities)
            {
                entity.Versions = versions;
            }

            return entities;
        }

        //private IList<Resource> BuildResourceFromQuery(SparqlParameterizedString parameterizedString, Uri pidUri = null)
        //{
        //    using var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(100));

        //    var resultsTask = Task.Factory.StartNew(() => _tripleStoreRepository.QueryTripleStoreResultSet(parameterizedString), cancellationTokenSource.Token);
        //    var versionsTask = Task.Factory.StartNew(() => GetAllVersionsOfResourceByPidUri(pidUri), cancellationTokenSource.Token);

        //    WaitAllTasks(cancellationTokenSource, resultsTask, versionsTask);

        //    SparqlResultSet results = resultsTask.Result;
        //    IList<VersionOverviewCTO> versions = versionsTask.Result;

        //    if (!results.Any())
        //    {
        //        throw new EntityNotFoundException("No resource found for given pid uri", pidUri.ToString());
        //    }

        //    var entities = TransformQueryResults(results, pidUri?.ToString());

        //    foreach (var entity in entities)
        //    {
        //        entity.Versions = versions;
        //    }

        //    return entities;
        //}

        private static void WaitAllTasks(CancellationTokenSource cancellationTokenSource, params Task[] tasks)
        {
            // OperationCanceledException will be thrown if time of token expired
            Task.WaitAll(tasks, cancellationTokenSource.Token);
        }

        private IList<Resource> TransformQueryResults(SparqlResultSet results,  ISet<Uri> namedGraphs, string id = "")
        {
            if (results.IsEmpty)
            {
                return new List<Resource>();
            }

            var groupedResults = results.GroupBy(result => result.GetNodeValuesFromSparqlResult("subject").Value);

            var counter = 0;
            var inboundCounter = 0;

            return groupedResults.Select(result => CreateResourceFromGroupedResult(result, counter, inboundCounter, namedGraphs)).ToList();
        }

        private Resource CreateResourceFromGroupedResult(IGrouping<string, SparqlResult> result, int counter, int inboundCounter, ISet<Uri> namedGraphs)
        {
            var id = result.Key;

            var newEntity = new Resource
            {
                Id = id,
                PublishedVersion = result.FirstOrDefault().GetNodeValuesFromSparqlResult("publishedVersion").Value,
                Properties = GetEntityPropertiesFromSparqlResultByList(result, id, counter),
                InboundProperties = GetInboundEntityPropertiesFromSparqlResultByList(result, inboundCounter)
            };

            newEntity.Versions = GetAllVersionsOfResourceByPidUri(newEntity.PidUri, namedGraphs);

            return newEntity;
        }

        private IDictionary<string, List<dynamic>> GetEntityPropertiesFromSparqlResultByList(IGrouping<string, SparqlResult> sparqlResults, string id, int counter)
        {
            // sparqlResults are a list of all properties of one resource inkl. subentites
            counter++;
            // filtered for actual entity
            var filteredResults = sparqlResults.Where(t => t.GetNodeValuesFromSparqlResult("object").Value == id).Where(s => s.GetNodeValuesFromSparqlResult("object_").Value != null);

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
                            var checkfilteredResults = sparqlResults.Where(t => t.GetNodeValuesFromSparqlResult("object").Value == valueProperty.Value);
                            var checkGroupedFilteredResults = checkfilteredResults.GroupBy(t => t.GetNodeValuesFromSparqlResult("predicate").Value);

                            if (checkGroupedFilteredResults.Any(x => x.Key == null))
                            {
                                value = string.IsNullOrWhiteSpace(valuePropertyPidUri.Value) ? valueProperty.Value : valuePropertyPidUri.Value;
                            }
                            else
                            {
                                value = new Entity()
                                {
                                    Id = valueProperty.Value,
                                    Properties = GetEntityPropertiesFromSparqlResultByList(sparqlResults, valueProperty.Value, counter)
                                };
                            }
                                
                        }
                        else
                        {
                            value = string.IsNullOrWhiteSpace(valuePropertyPidUri.Value) ? valueProperty.Value : valuePropertyPidUri.Value;
                        }

                        return value;
                    }).Distinct().ToList();
                });
        }

        private static IDictionary<string, List<dynamic>> GetInboundEntityPropertiesFromSparqlResultByList(IGrouping<string, SparqlResult> sparqlResults, int counter)
        {
            // sparqlResults are a list of all properties of one resource inkl. subentites
            counter++;
            // filtered for actual entity and no laterVersion
            var filteredResults = sparqlResults.Where(t => t.GetNodeValuesFromSparqlResult("inbound").Value == Graph.Metadata.Constants.Boolean.True && t.GetNodeValuesFromSparqlResult("inboundPredicate").Value != Graph.Metadata.Constants.Resource.HasLaterVersion);

            var groupedResults = filteredResults.GroupBy(t => t.GetNodeValuesFromSparqlResult("inboundPredicate").Value);

            return groupedResults.ToDictionary(
                res => res.FirstOrDefault().GetNodeValuesFromSparqlResult("inboundPredicate").Value,
                res => res.Select(t => t.GetNodeValuesFromSparqlResult("inboundPidUri")?.Value).Distinct().Cast<dynamic>().ToList());
        }

        public IList<VersionOverviewCTO> GetAllVersionsOfResourceByPidUri(Uri pidUri, ISet<Uri> namedGraph )
        {
            if (pidUri == null)
            {
                return new List<VersionOverviewCTO>();
            }

            var parameterizedString = new SparqlParameterizedString
            {
                CommandText = @"SELECT DISTINCT ?resource ?pidUri ?version ?laterVersion ?baseUri ?entryLifecycleStatus ?publishedResource
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
                      OPTIONAL { ?resource @hasLaterVersion ?laterVersion } .
                  } UNION {
                      ?subject @hasLaterVersion* ?resource.
                      ?resource pid3:hasVersion ?version .
                      ?resource @hasPid ?pidUri .
                      ?resource @hasEntryLifecycleStatus ?entryLifecycleStatus.
                      OPTIONAL { ?resource @hasBaseUri ?baseUri }.
                      OPTIONAL { ?publishedResource @hasPidEntryDraft ?resource }.
                      OPTIONAL { ?resource @hasLaterVersion ?laterVersion } .
                  }
                  Filter NOT EXISTS { ?draftResource  @hasPidEntryDraft ?resource}
                  }"
            };

            // Select all resources with their PID and target Url, which are of type resource and published

            parameterizedString.SetPlainLiteral("fromResourceNamedGraph", namedGraph.JoinAsFromNamedGraphs());
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
                PublishedVersion = result.GetNodeValuesFromSparqlResult("publishedResource")?.Value,
                LaterVersion = result.GetNodeValuesFromSparqlResult("laterVersion")?.Value
            }).ToList();

            //Sort Version ,considering laterVersion info
            var sortedResourceVersion = new List<VersionOverviewCTO>();
            string curLaterVersion = null;
            for (int i = 0; i < resourceVersions.Count; i++)
            {
                var curResource = resourceVersions.Where(x => x.LaterVersion == curLaterVersion).ToList();
                if (curResource.Count > 0)
                {
                    sortedResourceVersion.AddRange(curResource);
                    curLaterVersion = curResource.FirstOrDefault().Id;
                }
            }
            sortedResourceVersion.Reverse();

            return sortedResourceVersion;
        }

        public Uri GetPidUriById(Uri id, Uri draftGraph, Uri publishedGraph)
        {
            SparqlParameterizedString parameterizedString = new SparqlParameterizedString
            {
                CommandText =
                @"SELECT DISTINCT ?pidUri
                  From @resourcePublishedGraph
                  From @resourceDraftGraph
                  WHERE { 
              @id @hasPid ?pidUri.
  			  ?pidUri rdf:type pid2:PermanentIdentifier
                    }"
            };

            parameterizedString.SetUri("resourcePublishedGraph", publishedGraph);
            parameterizedString.SetUri("resourceDraftGraph", draftGraph);
            parameterizedString.SetUri("hasPid", new Uri(Graph.Metadata.Constants.EnterpriseCore.PidUri));
            parameterizedString.SetUri("id", id);

            var result = _tripleStoreRepository.QueryTripleStoreResultSet(parameterizedString).FirstOrDefault();
            if (result.IsNullOrEmpty() || !result.Any())
            {
                return null;
            }

            return new Uri(result.GetNodeValuesFromSparqlResult("pidUri").Value);
        }
    }
}
