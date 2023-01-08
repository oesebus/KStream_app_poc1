using Com.Evs.Pam.Service.Monitoringbridge.Api;
using Microsoft.AspNetCore.Mvc;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.State;

namespace Oesebus.Order.Application.Host.Controllers
{
  [Route("api/[controller]")]
  [ApiController]
  public class DiscussionController : ControllerBase
  {
    private readonly KafkaStream _app;

    //private readonly Istream
    public DiscussionController(KafkaStream app)
    {
      _app = app;
    }

    // GET: api/<DiscussionController>
    [HttpGet("key")]
    public ActionResult<MonitoringEvent> Get(string key)
    {
      var MonitoringEventStoreBySource = _app.Store
        (StoreQueryParameters.FromNameAndType("MonitoringEventCountBySource", QueryableStoreTypes.KeyValueStore<string, long>()));

      long? count = MonitoringEventStoreBySource.Get(key);

      return count is not null ?
        (ActionResult<MonitoringEvent>)Ok(new { key, count })
        : (ActionResult<MonitoringEvent>)NotFound(key);
    }

    // GET api/<dicussionController>/5
    //[HttpGet("{~/discussion/views}")]
    //public ActionResult<IEnumerable<MonitoringEvent>> ViewHighCountsOnly(long max)
    //{
    //  var MonitoringEventStoreBySource = _app.Store 
    //    (StoreQueryParameters.FromNameAndType("MonitoringEventCountBySource", QueryableStoreTypes.KeyValueStore<string, long>()));

    //  var counts = MonitoringEventStoreBySource.All().Where((c) => c.Value > max );
    //  return counts is not null ?
    //  (ActionResult<IEnumerable<MonitoringEvent>>)Ok(new { key, count })
    //    : (ActionResult<IEnumerable<MonitoringEvent>>)NotFound(key);
    //}

    // POST api/<OrdersController>
    //[HttpPost]
    //public void ProcessOrder([FromBody] InvoiceSerdes payload)
    //{

    //}

    // PUT api/<OrdersController>/5
    [HttpPut("{id}")]
    public void Put(int id, [FromBody] string value)
    {
    }

    // DELETE api/<OrdersController>/5
    [HttpDelete("{id}")]
    public void Delete(int id)
    {
    }
  }
}
