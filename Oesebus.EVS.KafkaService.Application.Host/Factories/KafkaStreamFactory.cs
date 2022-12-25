using System.Collections.Generic;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.Stream;

namespace Oesebus.EVS.KafkaService.Application.Host.Factories;

public static class KafkaStreamFactory
{
  public static IEnumerable<KafkaStream> Create(IEnumerable<Topology> topologies, IStreamConfig config)
  {
    foreach (var topo in topologies)
    {
      yield return new KafkaStream(topo, config);
    }
  }
}
