using System;
using System.Collections.Generic;
using COLID.Graph.Metadata.DataModels.Resources;

namespace COLID.IndexingCrawlerService.Repositories.Interface
{
    public interface IResourceRepository
    {
        bool CheckIfResourceExist(Uri pidUri, out string lifeCycleStatus);

        Resource GetPublishedResourceByPidUri(Uri pidUri);

        IList<Uri> GetAllPublishedPidUris();
    }
}
