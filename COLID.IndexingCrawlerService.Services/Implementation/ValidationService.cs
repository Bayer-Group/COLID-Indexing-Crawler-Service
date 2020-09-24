using System.Collections.Generic;
using System.Threading.Tasks;
using COLID.Graph.Metadata.DataModels.Metadata;
using COLID.Graph.Metadata.DataModels.Validation;
using COLID.Graph.Metadata.Services;
using COLID.Graph.TripleStore.DataModels.Base;

namespace COLID.IndexingCrawlerService.Services.Implementation
{
    internal class ValidationService : IValidationService
    {
        public IList<ValidationResultProperty> CheckForbiddenProperties<TEntity>(TEntity entity) where TEntity : Entity
        {
            throw new System.NotImplementedException();
        }

        public void CheckInstantiableEntityType<TEntity>(TEntity entity) where TEntity : EntityBase
        {
            throw new System.NotImplementedException();
        }

        public void CheckType<TEntity>(TEntity entity) where TEntity : EntityBase
        {
            throw new System.NotImplementedException();
        }

        public Task<ValidationResult> ValidateEntity(Entity entity, IList<MetadataProperty> metadataProperties, bool markedResultsAsCritical = false)
        {
            throw new System.NotImplementedException();
        }
    }
}
