using COLID.Graph.TripleStore.DataModels.Base;
using COLID.Graph.TripleStore.Extensions;

namespace COLID.IndexingCrawlerService.Services.Extensions
{
    public static class EntityExtension
    {
        public static bool IsPublishedResource(this Entity entity)
        {
            string linkedEntityLifeCycleStatus = entity.Properties.GetValueOrNull(Graph.Metadata.Constants.Resource.HasEntryLifecycleStatus, true);

            return linkedEntityLifeCycleStatus == Graph.Metadata.Constants.Resource.ColidEntryLifecycleStatus.Published || linkedEntityLifeCycleStatus == Graph.Metadata.Constants.Resource.ColidEntryLifecycleStatus.MarkedForDeletion;
        }
    }
}
