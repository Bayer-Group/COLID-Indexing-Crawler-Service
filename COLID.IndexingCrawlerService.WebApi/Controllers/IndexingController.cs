using System.Net.Mime;
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
        public IActionResult StartReindex()
        {
            _indexingService.StartReindex();

            return Ok();
        }
    }
}
