using System.Collections.Generic;
using COLID.Graph.TripleStore.DataModels.Base;

namespace COLID.IndexingCrawlerService.Repositories.Interface
{
    public interface IEntityRepository
    {
        Entity GetEntityById(string id, string propertyKey);

        IList<Entity> GetEntities(string type);
    }
}
