using System.Collections.Generic;
using COLID.Graph.TripleStore.DataModels.Base;

namespace COLID.IndexingCrawlerService.Services.Interface
{
    public interface IEntityService
    {
        /// <summary>
        /// Searches for entities filtered by given entity type.
        /// </summary>
        /// <param name="entityType">Entity type to search for</param>
        /// <returns>List of entities matching the entity type</returns>
        IList<BaseEntityResultDTO> GetEntities(string entityType);

        /// <summary>
        /// Generates the generic entity, identified by the given id.
        /// </summary>
        /// <param name="id">the id to search for</param>
        /// <param name="propertyKey">the key of id to search for</param>
        /// <returns>the found entity, otherwise null</returns>
        BaseEntityResultDTO GetEntity(string id, string propertyKey);
    }
}
