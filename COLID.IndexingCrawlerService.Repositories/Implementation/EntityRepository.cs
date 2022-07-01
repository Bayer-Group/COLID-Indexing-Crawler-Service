using System;
using System.Collections.Generic;
using System.Linq;
using COLID.Graph.Metadata.Repositories;
using COLID.Graph.TripleStore.Repositories;
using COLID.IndexingCrawlerService.Repositories.Interface;
using VDS.RDF.Query;
using COLID.Graph.TripleStore.DataModels.Base;
using COLID.Common.Extensions;
using COLID.Graph.TripleStore.DataModels.Attributes;
using COLID.Graph.TripleStore.Extensions;

namespace COLID.IndexingCrawlerService.Repositories.Implementation
{
    public class EntityRepository : IEntityRepository
    {
        private string InsertingGraph => Graph.Metadata.Constants.MetadataGraphConfiguration.HasMetadataGraph;

        private IEnumerable<string> QueryGraphs => new List<string>() {
            Graph.Metadata.Constants.MetadataGraphConfiguration.HasMetadataGraph,
            Graph.Metadata.Constants.MetadataGraphConfiguration.HasConsumerGroupGraph,
            Graph.Metadata.Constants.MetadataGraphConfiguration.HasPidUriTemplatesGraph,
            Graph.Metadata.Constants.MetadataGraphConfiguration.HasKeywordsGraph,
            Graph.Metadata.Constants.MetadataGraphConfiguration.HasInstanceGraph,
            Graph.Metadata.Constants.MetadataGraphConfiguration.HasExtendedUriTemplateGraph
        };

        private string Type => typeof(Entity).GetAttributeValue((TypeAttribute type) => type.Type);

        private readonly ITripleStoreRepository _tripleStoreRepository;

        private readonly IMetadataGraphConfigurationRepository _metadataGraphConfigurationRepository;

        public EntityRepository(ITripleStoreRepository tripleStoreRepository, IMetadataGraphConfigurationRepository metadataGraphConfigurationRepository)
        {
            _tripleStoreRepository = tripleStoreRepository;
            _metadataGraphConfigurationRepository = metadataGraphConfigurationRepository;
        }

        public IList<Entity> GetEntities(string type)
        {
            var query = GenerateGetAllQuery(type, QueryGraphs);

            var results = _tripleStoreRepository.QueryTripleStoreResultSet(query);

            return TransformQueryResults(results);
        }

        private SparqlParameterizedString GenerateGetAllQuery(string type, IEnumerable<string> namedGraphs)
        {
            if (string.IsNullOrWhiteSpace(type))
            {
                return null;
            }

            var parameterizedString = new SparqlParameterizedString();
            parameterizedString.CommandText =
                @"SELECT ?subject ?predicate ?object
                  @fromNamedGraphs
                  WHERE {
                      ?subject rdf:type [rdfs:subClassOf* @type].
                      ?subject ?predicate ?object
                  }";

            parameterizedString.SetPlainLiteral("fromNamedGraphs", GetNamedSubGraphs(namedGraphs));
            parameterizedString.SetUri("type", new Uri(type));

            return parameterizedString;
        }

        private SparqlParameterizedString GenerateGetQuery(string id, IEnumerable<string> namedGraphs)
        {
            if (string.IsNullOrWhiteSpace(id))
            {
                return null;
            }

            var parameterizedString = new SparqlParameterizedString();
            parameterizedString.CommandText =
                @"SELECT ?predicate ?object
                  @fromNamedGraphs
                  WHERE {
                      @subject ?predicate ?object
                  }";

            parameterizedString.SetPlainLiteral("fromNamedGraphs", GetNamedSubGraphs(namedGraphs));
            parameterizedString.SetUri("subject", new Uri(id));

            return parameterizedString;
        }

        public Entity GetEntityById(string id)
        {
            var query = GenerateGetQuery(id, QueryGraphs);

            if (query == null)
            {
                return null;
            }

            var results = _tripleStoreRepository.QueryTripleStoreResultSet(query);

            if (!results.Any())
            {
                return null;
            }
            return TransformQueryResults(results, id).FirstOrDefault();
        }

        private string GetNamedSubGraphs(IEnumerable<string> namedGraphs)
        {
            if (namedGraphs == null || !namedGraphs.Any())
            {
                throw new ArgumentException("No query graphs specified");
            }

            if (_metadataGraphConfigurationRepository == null)
            {
                return namedGraphs.JoinAsFromNamedGraphs();
            }

            return _metadataGraphConfigurationRepository.GetGraphs(namedGraphs).JoinAsFromNamedGraphs();
        }

        private IList<Entity> TransformQueryResults(SparqlResultSet results, string id = "")
        {
            if (results.IsEmpty)
            {
                return null;
            }

            var groupedResults = results.GroupBy(result => result.GetNodeValuesFromSparqlResult("subject").Value);

            IList<Entity> foundEntities = groupedResults.Select(result =>
            {
                var subGroupedResults = result.GroupBy(res => res.GetNodeValuesFromSparqlResult("predicate").Value);
                var newEntity = new Entity
                {
                    Id = id == string.Empty ? result.Key : id,
                    Properties = subGroupedResults.ToDictionary(x => x.Key, x => x.Select(property => GetEntityPropertyFromSparqlResult(property)).ToList())
                };

                return newEntity;
            }).ToList();

            return foundEntities;
        }

        private dynamic GetEntityPropertyFromSparqlResult(SparqlResult res)
        {
            return res.GetNodeValuesFromSparqlResult("object").Value;
        }
    }
}
