using Microsoft.Extensions.Hosting;
using Oesebus.EVS.KafkaService.Application.Core.Interfaces;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.Stream;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Oesebus.EVS.KafkaService.Application.Host.Workers
{
  public class DefaultTopologyStreamWorker : IHostedService
  {
    private readonly KafkaStream _app;
    private readonly IBuilder _toplogyBuilder;
    private readonly IStreamConfig _streamConfig;
    public DefaultTopologyStreamWorker(KafkaStream apps, IBuilder topology, IStreamConfig streamConfig)
    {
      _app = apps;
      _toplogyBuilder = topology;
      _streamConfig = streamConfig;
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
      Task.Run(async () => await StartKafkaStreamsAsync(cancellationToken).ConfigureAwait(false), cancellationToken);

      return Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
      _app.Dispose();

      return Task.CompletedTask;
    }
    private async Task StartKafkaStreamsAsync(CancellationToken cancellationToken)
    {
        await _toplogyBuilder.Prerequisites(_streamConfig).ConfigureAwait(false);
        await _app.StartAsync(cancellationToken);
    }
  }
}
