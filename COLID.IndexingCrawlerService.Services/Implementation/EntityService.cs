using System.Collections.Generic;
using System.Linq;
using AutoMapper;
using COLID.Common.Extensions;
using COLID.Graph.TripleStore.DataModels.Attributes;
using COLID.Graph.TripleStore.DataModels.Base;
using COLID.IndexingCrawlerService.Repositories.Interfaces;
using COLID.IndexingCrawlerService.Services.Interfaces;

namespace COLID.IndexingCrawlerService.Services.Implementation
{
    public class EntityService : IEntityService
    {
        private static string Type => typeof(Entity).GetAttributeValue((TypeAttribute type) => type.Type);

        private readonly IEntityRepository _entityRepository;

        private readonly IMapper _mapper;

        public EntityService(IEntityRepository entityRepository, IMapper mapper)
        {
            _entityRepository = entityRepository;
            _mapper = mapper;
        }

        public BaseEntityResultDTO GetEntity(string id, string propertyKey)
        {
            var entity = _entityRepository.GetEntityById(id, propertyKey);

            return entity == null ? null : _mapper.Map<BaseEntityResultDTO>(entity);
        }

        public IList<BaseEntityResultDTO> GetEntities(string entityType)
        {
            var entitites = _entityRepository.GetEntities(entityType);

            if (entitites == null)
            {
                return new List<BaseEntityResultDTO>();
            }

            return entitites.Select(c => _mapper.Map<BaseEntityResultDTO>(c)).OrderBy(c => c.Name).ToList();
        }
    }
}
