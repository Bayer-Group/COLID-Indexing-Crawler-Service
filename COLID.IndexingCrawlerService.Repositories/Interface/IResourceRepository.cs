using System;
using System.Collections.Generic;
using COLID.Graph.Metadata.DataModels.Resources;
using COLID.RegistrationService.Common.DataModel.Resources;

namespace COLID.IndexingCrawlerService.Repositories.Interface
{
    public interface IResourceRepository
    {
        bool CheckIfResourceExist(Uri pidUri, out string lifeCycleStatus);

        IList<Uri> GetAllPidUris();

        IList<VersionOverviewCTO> GetAllVersionsOfResourceByPidUri(Uri pidUri);

        /// <summary>
        /// Gets both lifecycle states (draft and published) of a resource, if present.
        /// </summary>
        /// <param name="pidUri">The unique PID URI of the resource</param>
        /// <param name="resourceTypes">the resource type list to filter by</param>
        /// <returns>A transport object containing two different lifecycle versions of resources</returns>
        ResourcesCTO GetResourcesByPidUri(Uri pidUri, IList<string> resourceTypes);
    }
}
