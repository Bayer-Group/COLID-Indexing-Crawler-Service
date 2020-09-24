using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;

namespace COLID.IndexingCrawlerService.Repositories.DataModel
{
    public class ResourceType
    {
        [Required]
        public string Key { get; set; }

        public string Label { get; set; }

        public string Description { get; set; }

        public IList<ResourceType> SubClasses { get; set; }

        public ResourceType(string key, string label, string description)
        {
            Key = key;
            Label = label;
            Description = description;
            SubClasses = new List<ResourceType>();
        }
    }
}
