using Microsoft.AspNetCore.Mvc;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using elasticsearch_integration.Context;
using Microsoft.EntityFrameworkCore;
using Elasticsearch.Net;
using Newtonsoft.Json.Linq;

namespace elasticsearch_integration.Controllers
{
    [Route("api/[controller]")]
    public class ValuesController : Controller
    {
        AppDbContext context = new();
        [HttpGet("[action]")]
        public async Task<IActionResult> CreateData(CancellationToken token)
        {
            var travels = new List<Travel>();
            var random = new Random();
            for (int i = 0; i < 10000; i++)
            {
                var title = new string(Enumerable.Repeat("abcdefghjkliyufdgdgfhgg", 5)
                    .Select(x => x[random.Next(x.Length)]).ToArray());
                var words = new List<string>();
                for (int j = 0; j < 500; j++)
                {
                    words.Add(new string(Enumerable.Repeat("abcdefghjkliyufdgdgfhgg", 5)
                        .Select(x => x[random.Next(x.Length)]).ToArray()));
                }

                var description = string.Join(" ", words);
                var travel = new Travel()
                {
                    Title = title,
                    Description = description
                };
                travels.Add(travel);
            }

            await context.Set<Travel>().AddRangeAsync(travels, token);
            await context.SaveChangesAsync(token);
            return Ok();
        }

        [HttpGet("[action]/{description}")]
        public async Task<IActionResult> GetData(string description)
        {
            var data = await context.Set<Travel>()
                .Where(x=>x.Description.Contains(description))
                .AsNoTracking().ToListAsync();
            return Ok(data.Take(10));
        }

        [HttpGet("[action]")]
        public async Task<IActionResult> SyncToElastic()
        {
            var settings = new ConnectionConfiguration(new Uri("http://localhost:9200"));

            var client = new ElasticLowLevelClient(settings);

            List<Travel> travels = await context.Travels.ToListAsync();

            var tasks = new List<Task>();

            foreach (var travel in travels)
            {
                tasks.Add(client.IndexAsync<StringResponse>("travels", travel.Id.ToString(), PostData.Serializable(new
                {
                    travel.Id,
                    travel.Title,
                    travel.Description
                })));
            }

            await Task.WhenAll(tasks);

            return Ok();
        }

        [HttpGet("[action]/{value}")]
        public async Task<IActionResult> GetDataListWithElasticSearch(string value)
        {
            var settings = new ConnectionConfiguration(new Uri("http://localhost:9200"));

            var client = new ElasticLowLevelClient(settings);

            var response = await client.SearchAsync<StringResponse>("travels", PostData.Serializable(new
            {
                query = new
                {
                    wildcard = new
                    {
                        Description = new { value = $"*{value}*" }
                    }
                }
            }));

            var results = JObject.Parse(response.Body);

            var hits = results["hits"]["hits"].ToObject<List<JObject>>();

            List<Travel> travels = new();

            foreach (var hit in hits)
            {
                travels.Add(hit["_source"].ToObject<Travel>());
            }

            return Ok(travels.Take(10));
        }
    }
}
