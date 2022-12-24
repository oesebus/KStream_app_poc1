using System;
using System.ComponentModel;
using Microsoft.Extensions.DependencyInjection;
using Oesebus.Order.Application.Core.Interfaces;
using Oesebus.Order.Infrastructure.Services;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.Stream;

namespace Oesebus.EVS.KafkaService.Application.Host;

public static class ToplogyBuilderExtensions
{
  public static IServiceCollection AddToplogy(this IServiceCollection container , IBuilder builder)
  {
    container.AddSingleton(builder.Build());
    return container;
  }
}
