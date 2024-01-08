using AutoMapper;
using COLID.Graph.Metadata.DataModels.MessageQueuing;
using COLID.Graph.Metadata.DataModels.Metadata;

namespace COLID.IndexingCrawlerService.Services.MappingProfiles
{
    public class MetadataPropertyProfile : Profile
    {
        public MetadataPropertyProfile()
        {
            CreateMap<MetadataProperty, MetadataPropertyDTO>();
        }
    }
}
