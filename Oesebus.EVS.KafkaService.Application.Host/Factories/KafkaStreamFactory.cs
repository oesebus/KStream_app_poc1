using System.Collections.Generic;
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
}
