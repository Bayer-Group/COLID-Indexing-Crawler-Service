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

        /// <summary>
        /// Index a document
        /// </summary>
        /// <param name="indexedResourceString">resource in json string format</param>
        Task IndexResourceFromTopic(string indexedResourceString);

        /// <summary>
        /// Fetch pidUris from SQS and start Indexing
        /// </summary>        
        void ReindexResource(string action);

        // <summary>
        /// Fetch Resource DTO from SQS and Index
        /// </summary>        
        void IndexResource();
    }
}
