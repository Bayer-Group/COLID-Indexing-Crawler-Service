using System;
using System.Collections.Generic;
using System.Text;
using COLID.Common.Enums;
using COLID.Graph.Metadata.Constants;
using COLID.Graph.Metadata.DataModels.Metadata;
using COLID.Graph.Metadata.Extensions;
using COLID.Graph.TripleStore.Extensions;
using Microsoft.VisualBasic;

namespace COLID.IndexingCrawlerService.Services.Extensions
{
    public static class MetadataPropertyIListExtension
    {
        public static IList<MetadataProperty> ModifyPropertiesForElastic(this IList<MetadataProperty> properties)
        {
            foreach (var metadataProperty in properties)
            {
                var facetAlways = IsFacetDataType(metadataProperty) || IsNodeKindIri(metadataProperty);
                var facetOnlySearch = IsClassOrRangePerson(metadataProperty) || IsFacetOnlySearchPath(metadataProperty);
                var facetExclude = IsNestedObjectEditor(metadataProperty) || IsLinkedTypesGroup(metadataProperty) || IsFacetExcludePaths(metadataProperty);

                if (facetExclude)
                {
                    metadataProperty.Properties.AddOrUpdate(Resource.IsFacet, FacetType.Never);
                }
                else if (facetOnlySearch)
                {
                    metadataProperty.Properties.AddOrUpdate(Resource.IsFacet, FacetType.OnlySearch);
                }
                else if (facetAlways)
                {
                    metadataProperty.Properties.AddOrUpdate(Resource.IsFacet, FacetType.Always);
                }
            }

            return properties;
        }

        private static bool IsFacetDataType(MetadataProperty property)
        {
            if (property.Properties.TryGetValue(Shacl.Datatype, out var dataType))
            {
                return dataType == DataTypes.Boolean || dataType == DataTypes.DateTime;
            }

            return false;
        }

        private static bool IsNodeKindIri(MetadataProperty property)
        {
            if (property.Properties.TryGetValue(Shacl.NodeKind, out var nodeKind))
            {
                return nodeKind == Shacl.NodeKinds.IRI;
            }

            return false;
        }

        private static bool IsClassOrRangePerson(MetadataProperty property)
        {
            // Is needed because the variable is not initialized by the first if condition.
            dynamic personClass = string.Empty;

            if (property.Properties.TryGetValue(Shacl.Range, out var personRange) ||
                property.Properties.TryGetValue(Shacl.Class, out personClass))
            {
                return personRange == EnterpriseCore.Person || personClass == EnterpriseCore.Person;
            }

            return false;
        }

        private static bool IsNestedObjectEditor(MetadataProperty property)
        {
            if (property.Properties.TryGetValue(TopBraid.EditWidget, out var editWidget))
            {
                return editWidget == TopBraid.NestedObjectEditor;
            }

            return false;
        }

        private static bool IsLinkedTypesGroup(MetadataProperty property)
        {
            var shaclGroup = property.GetMetadataPropertyGroup();
            return null != shaclGroup && shaclGroup.Key == Resource.Groups.LinkTypes;
        }

        private static bool IsFacetOnlySearchPath(MetadataProperty property)
        {
            var facetOnlySearchPaths = new List<string>()
            {
                Resource.HasVersion, Resource.Author, Resource.LastChangeUser, Resource.HasDataSteward
            };
            return facetOnlySearchPaths.Contains(property.Key);
        }

        private static bool IsFacetExcludePaths(MetadataProperty property)
        {
            var facetExcludePaths = new List<string>()
            {
                EnterpriseCore.PidUri,
                Resource.BaseUri,
                Resource.HasEntryLifecycleStatus,
                Resource.HasHistoricVersion,
                Resource.MetadataGraphConfiguration
            };

            return facetExcludePaths.Contains(property.Key);
        }
    }
}
