using System;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Google.Protobuf;
using Microsoft.Extensions.Hosting;
using Oesebus.EVS.KafkaService.Application.Host.Factories;

namespace Oesebus.EVS.KafkaService.Application.Host.Workers;

public class ConsumeWorker<T> : IHostedService where T : IMessage
{
  private string Topic { get; set; }
  private ClientConfig ClientConfig { get; }
  private string Identifier { get; }
  private ConsumerConfig ConsumerConfig { get; set; }

  private IConsumer<string, T> Instance { get; set; }

  public ConsumeWorker(string topic, ClientConfig clientConfig)
  {
    Topic = topic;
    ClientConfig = clientConfig;
    ConsumerConfig = ConsumerConfigFactory.Create(ClientConfig);
    Identifier = ConsumerConfig.GroupId;
  }

  public Task StartAsync(CancellationToken cancellationToken)
  {

    var cts = new CancellationTokenSource();

    Console.CancelKeyPress += (_, e) =>
    {
      e.Cancel = true; // prevent the process from terminating.
      cts.Cancel();
    };

    Instance = ConsumerFactory.Create<string, T>(ConsumerConfig);

    Instance.Subscribe(Topic);

    try
    {
      while (!cts.Token.IsCancellationRequested)
      {
        var cr = Instance.Consume(cts.Token);
        Console.WriteLine($"Consumed record with key {cr.Message.Key} and value {System.Text.Json.JsonSerializer.Serialize(cr.Message.Value)}.");
      }
    }
    catch (OperationCanceledException)
    {
      // Ctrl-C was pressed.
    }
    finally
    {
      Instance.Close();
    }
    return Task.CompletedTask;
  }
  public Task StopAsync(CancellationToken cancellationToken)
  {
    Instance.Close();

    Console.WriteLine($"Terminating Consumer ID {Identifier}");
    Console.Read();

    return Task.CompletedTask;
  }
}