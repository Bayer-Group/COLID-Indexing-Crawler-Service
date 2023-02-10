using System;
using System.Collections.Generic;
using COLID.Graph.Metadata.DataModels.Resources;
using COLID.RegistrationService.Common.DataModel.Resources;

namespace COLID.IndexingCrawlerService.Services.Interface
{
    public interface IResourceService
    {
        /// <summary>
        /// Gets the pid uri of all resources.
        /// </summary>
        /// <returns>The pid uris of all resource</returns>
        IList<Uri> GetAllPidUris();

        IList<VersionOverviewCTO> GetAllVersionsOfResourceByPidUri(Uri pidUri);

        /// <summary>
        /// Gets both lifecycle states (draft and published) of a resource, if present.
        /// </summary>
        /// <param name="pidUri">The unique PID URI of the resource</param>
        /// <returns>A transport object containing two different lifecycle versions of resources</returns>
        ResourcesCTO GetResourcesByPidUri(Uri pidUri, bool useCache);

        /// <summary>
        /// Resource for indexing are completely removed from the cache.
        /// </summary>
        /// <param name="pidUri">The unique PID URI of the resource to be removed</param>
        void DeleteCachedResource(Uri pidUri);

        /// <summary>
        /// Resources for indexing are completely removed from the cache.
        /// </summary>
        void DeleteCachedResources();
    }
}
