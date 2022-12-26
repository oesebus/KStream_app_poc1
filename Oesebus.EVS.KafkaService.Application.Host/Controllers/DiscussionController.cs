using Com.Evs.Pam.Service.Monitoringbridge.Api;
using Microsoft.AspNetCore.Mvc;
using Oesebus.EVS.KafkaService.Application.Core.SerDes;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State;
using System;
using System.Collections.Generic;


namespace Oesebus.Order.Application.Host.Controllers
{
  [Route("api/[evscontroller]")]
  [ApiController]
  public class DiscussionController : ControllerBase
  {
    private readonly KafkaStream _app;

    //private readonly Istream
    public DiscussionController(KafkaStream app)
    {
      _app = app;
    }

    // GET: api/<OrdersController>
    [HttpGet("key")]
    public ActionResult<MonitoringEvent> Get(string key)
    {
      var MonitoringEventStoreBySource = _app.Store
        (StoreQueryParameters.FromNameAndType("MonitoringEventCountBySource", QueryableStoreTypes.KeyValueStore<string, MonitoringEvent>()));

      var @event = MonitoringEventStoreBySource.Get(key);

      return Ok(@event);

    }

    // GET api/<OrdersController>/5
    [HttpGet("{~/orders/pending}")]
    public string ViewPendingOrders()
    {
      return "value";
    }

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
