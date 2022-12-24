using Microsoft.Extensions.Logging;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.Stream;
using System.Threading.Tasks;

namespace Oesebus.Order.Application.Core.Interfaces
{
  public interface IBuilder
  {
    public Task Prerequisites(IStreamConfig config);
    public Topology Build();
  }
}