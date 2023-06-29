using System.Threading.Tasks;

namespace COLID.IndexingCrawlerService.Services.Interfaces
{
    public interface IIndexingService
    {
        /// <summary>
        /// Starts re-indexing the elastic by fetching all metadata and sending it to dmp.
        /// Then all available publsihed pid uris are sent to the mq for reindexing.
        /// </summary>
        Task StartReindex();
    }
}
