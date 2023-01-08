using System;
using Confluent.Kafka;
using Google.Protobuf;
using Microsoft.Extensions.DependencyInjection;
using Oesebus.EVS.KafkaService.Application.Host.Workers;

namespace Oesebus.EVS.KafkaService.Application.Host.Interfaces;

public static class KafkaConsumerBuilderHelper
{
  public static IServiceCollection AddConsumerService<K, V>(this IServiceCollection services, ClientConfig config, string topicName)
    where K : class
    where V : IMessage
  {
    var configs = new ConsumerConfig
    {
      BootstrapServers = "localhost:9094",
      GroupId = $"OesebusService-{Guid.NewGuid()}",
      AutoOffsetReset = AutoOffsetReset.Earliest
    };

    services.AddHostedService((_) => new ConsumeWorker<V>(topicName, config));

    return services;
  }
}

