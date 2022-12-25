using System;
using Confluent.Kafka;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.EventLog;
using Oesebus.EVS.KafkaService.Application.Core.Interfaces;
using Oesebus.EVS.KafkaService.Application.Host;
using Oesebus.EVS.KafkaService.Application.Host.Factories;
using Oesebus.EVS.KafkaService.Application.Host.Workers;
using Oesebus.Order.Infrastructure.Services;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Stream;

namespace Oesebus.Order.Application.Host.IoC
{
  public static class ContainerConfiguration
  {
    public static IServiceCollection Initialize(IServiceCollection Container)
    {
      var toplogylogger = LoggerFactory.Create((logger) => logger.AddDebug().AddConsole()).CreateLogger<EvsDicussionTopologyBuilder>();

      Container.AddSingleton<ILogger>(toplogylogger)
               .AddSingleton(StreamConfigurator.Initialize())
               .AddToplogy(new EvsDicussionTopologyBuilder())
               .AddHostedService<DefaultTopologyStreamWorker>();

      return Container;
    }
  }
  public static class StreamConfigurator
  {
    public static IStreamConfig Initialize()
    {
      var config = new StreamConfig<StringSerDes, StringSerDes>
      {
        /// Default ValueKeySerDes
        ApplicationId = "DiscussionViewTopology-POCree0501880s0187009",
        ClientId = Guid.NewGuid().ToString(),
        BootstrapServers = "127.0.0.1:9094",
        AutoOffsetReset = AutoOffsetReset.Earliest,
        FollowMetadata = true,
        NumStreamThreads = 2 //This specifies the number of stream threads in an instance of the Kafka Streams application. The stream processing code runs in these thread.
      };

      config.AllowAutoCreateTopics = false;
      config.Debug = "consumer,cgrp,fetch,topic,broker,eos,msg";
      //config.Acks = Acks.Leader;
      //config.LogQueue = true;
      config.HeartbeatIntervalMs = 3000;
      config.DeserializationExceptionHandler = ((context, _, ex) =>
      {
        Console.WriteLine("Exception => " + ex);
        Console.WriteLine("ProcessorContext => Topic : " + context.Topic);
        return ExceptionHandlerResponse.CONTINUE;
      });

      config.InnerExceptionHandler = (ex) =>
      {
        Console.WriteLine("Inner Exception handling => " + ex);

        return ExceptionHandlerResponse.CONTINUE;
      };
      config.ProductionExceptionHandler = (ex) =>
      {
        Console.WriteLine("Producer Exception handling => " + ex);

        return ExceptionHandlerResponse.CONTINUE;
      };

      config.Logger = LoggerFactory.Create(builder =>
      {
        builder.SetMinimumLevel(LogLevel.Trace);
        builder.AddEventLog(new EventLogSettings()
        {
          Filter = (_, level) => level == LogLevel.Debug
        });
      });

      return config;
    }
  }
}
