using System;
using System.Collections.Generic;
using COLID.Graph.Metadata.DataModels.Resources;

namespace COLID.IndexingCrawlerService.Services.Interface
{
    public interface IResourceService
    {
        bool CheckIfResourceExist(Uri pidUri, out string lifeCycleStatus);

        /// <summary>
        /// Gets the pid uri of all published resources.
        /// </summary>
        /// <returns>The pid uris of all published resource</returns>
        IList<Uri> GetAllPublishedPidUris();

        /// <summary>
        /// Gets the published resources with its properties and nested objects inlcusive linked resources.
        /// </summary>
        /// <param name="pidUri">The unique PID URI of the resource</param>
        /// <returns>The published resource containing the given PID URI</returns>
        Resource GetPublishedResourceByPidUri(Uri pidUri);
    }
}
