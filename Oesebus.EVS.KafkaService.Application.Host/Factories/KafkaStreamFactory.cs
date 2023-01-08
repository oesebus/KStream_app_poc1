using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Threading.Tasks;
using Oesebus.EVS.KafkaService.Application.Core.Interfaces;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.Stream;

namespace Oesebus.EVS.KafkaService.Application.Host.Factories;

public static class KafkaStreamFactory
{
  public static KafkaStream Create(IBuilder topologyBuilder, IStreamConfig config)
  {
    return new KafkaStream(topologyBuilder.Build(), config);
  }

  public static KafkaStream Create(Func<IStreamConfig, Topology> func, IStreamConfig config)
  {
    var topology = func.Invoke(config);

    return new KafkaStream(topology, config);
  }
}
