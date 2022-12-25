using Microsoft.Extensions.DependencyInjection;
using Oesebus.EVS.KafkaService.Application.Core.Interfaces;

namespace Oesebus.EVS.KafkaService.Application.Host;

public static class ToplogyBuilderExtensions
{
  public static IServiceCollection AddToplogy(this IServiceCollection container , IBuilder builder)
  {
    container.AddSingleton(builder.Build());
    return container;
  }
}
