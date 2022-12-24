using Microsoft.AspNetCore.Mvc;
using System.Collections.Generic;


namespace Oesebus.Order.Application.Host.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class DiscussionController : ControllerBase
    {

    //private readonly Istream
    public DiscussionController()
    {
        
    }

        // GET: api/<OrdersController>
        [HttpGet]
        public IEnumerable<string> Get()
        {
            return new string[] { "value1", "value2" };
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
