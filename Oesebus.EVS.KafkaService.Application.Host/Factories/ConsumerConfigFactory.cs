using System.Reflection;
using Confluent.Kafka;

namespace Oesebus.EVS.KafkaService.Application.Host.Factories;

public static class ConsumerConfigFactory
{
  public static ConsumerConfig Create(ClientConfig config)
  {
    var consumerConfig = new ConsumerConfig(config);
    consumerConfig.GroupId = $"{Assembly.GetExecutingAssembly().FullName}-KStreamsOutput";
    consumerConfig.AutoOffsetReset = AutoOffsetReset.Earliest;
    consumerConfig.EnableAutoCommit = false;

    return consumerConfig;
  }
}
