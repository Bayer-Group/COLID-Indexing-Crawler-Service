using System.Collections.Generic;
using System.Linq;
using COLID.Graph.Metadata.DataModels.Metadata;
using COLID.Graph.Metadata.DataModels.Resources;
using COLID.Graph.Metadata.Extensions;
using COLID.Graph.TripleStore.DataModels.Base;
using COLID.Graph.TripleStore.Extensions;

namespace COLID.IndexingCrawlerService.Services.Extensions
{
    public static class ResourceExtension
    {
        /// <summary>
        /// Searches the resource for all link types. Duplicate entries will be removed.
        /// </summary>
        /// <param name="reosurce">Pid entry to search in</param>
        /// <returns>Returns a list of all published links as pid uri</returns>
        public static IList<string> GetLinks(this Resource resource, IList<MetadataProperty> metadataProperties)
        {
            var linkedOutboundPidUris = GetAllLinkedResourceUris(resource.Properties, metadataProperties);

            var linkedInboundPidUris = GetAllLinkedResourceUris(resource.InboundProperties, metadataProperties);

            // Merges the linked pid uris and versions pid uris, and removes duplicate entries
            var linkedPidUris = linkedOutboundPidUris
                .Concat(linkedInboundPidUris)
                .Concat(resource.Versions.Select(property => property.PidUri))
                .Distinct()
                .ToList();

            return linkedPidUris;
        }

        /// <summary>
        /// Searches the properties for linked published entries including all nested objects
        /// </summary>
        /// <param name="properties">Properties of the entry to be searched</param>
        /// <param name="metadataProperties">Metadata of the entity type</param>
        /// <returns></returns>
        private static IList<string> GetAllLinkedResourceUris(IDictionary<string, List<dynamic>> properties, IList<MetadataProperty> metadataProperties)
        {
            var linkedUris = new List<string>();

            foreach (var propertyItem in properties)
            {
                foreach (var propertyValue in propertyItem.Value)
                {
                    var metadata = metadataProperties.FirstOrDefault(prop => prop.Properties[Graph.Metadata.Constants.EnterpriseCore.PidUri] == propertyItem.Key);

                    if (metadata?.GetMetadataPropertyGroup()?.Key == Graph.Metadata.Constants.Resource.Groups.LinkTypes)
                    {
                        if (propertyValue is Entity)
                        {
                            Entity linkedEntity = propertyValue;

                            if (linkedEntity.IsPublishedResource())
                            {
                                var pidUri = linkedEntity.Properties.GetValueOrNull(Graph.Metadata.Constants.EnterpriseCore.PidUri, true);
                                linkedUris.Add(pidUri is Entity ? pidUri.Id : pidUri);
                            }
                        }
                        else
                        {
                            linkedUris.Add(propertyValue);
                        }
                    }
                }
            }

            return linkedUris;
        }
    }
}
