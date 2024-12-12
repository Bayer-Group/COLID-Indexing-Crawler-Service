using System.Net.Mime;
using System.Threading.Tasks;
using COLID.IndexingCrawlerService.Services.Interfaces;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;

namespace COLID.IndexingCrawlerService.WebApi.Controllers
{
    /// <summary>
    /// API endpoint for indexing.
    /// </summary>
    [ApiController]
    [Produces(MediaTypeNames.Application.Json)]
    [Route("api/reindex")]
    [Authorize(Roles = "Resource.Index.All")]
    public class IndexingController : Controller
    {
        private readonly IIndexingService _indexingService;

        /// <summary>
        /// API endpoint for indexing.
        /// </summary>
        public IndexingController(IIndexingService ReindexingService)
        {
            _indexingService = ReindexingService;
        }

        /// <summary>
        /// Starts the indexing process so that the cache is cleared, a new index with the current metadata is created in Data Marketplace, and all resources are rewritten to the new index.
        /// </summary>
        /// <returns></returns>
        [HttpPost]
        public async Task<IActionResult> StartReindex()
        {
            await _indexingService.StartReindex();

            return Ok();
        }

        /// <summary>
        /// Index a document.
        /// </summary>
        /// <returns></returns>
        [HttpPut]
        public IActionResult IndexResource([FromBody] string resource)
        {
            _indexingService.IndexResourceFromTopic(resource);

            return Ok();
        }
    }
}
