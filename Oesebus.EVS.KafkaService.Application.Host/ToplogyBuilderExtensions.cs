using Microsoft.Extensions.DependencyInjection;
using Oesebus.EVS.KafkaService.Application.Core.Interfaces;
using Oesebus.EVS.KafkaService.Application.Host.Factories;
using Streamiz.Kafka.Net;

namespace Oesebus.EVS.KafkaService.Application.Host;

public static class ToplogyBuilderExtensions
{
  public static IServiceCollection AddToplogy(this IServiceCollection container, IBuilder builder)
  {
    container.AddSingleton(builder.Build())
             .AddSingleton(builder)
             .AddSingleton((serviceProvider) =>
              KafkaStreamFactory.Create(serviceProvider.GetRequiredService<IBuilder>(), serviceProvider.GetRequiredService<IStreamConfig>()));
    return container;
  }
}
