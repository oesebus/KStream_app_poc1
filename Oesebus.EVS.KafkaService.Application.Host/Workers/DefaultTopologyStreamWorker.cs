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
    private readonly IEnumerable<KafkaStream> _apps;
    private readonly IEnumerable<IBuilder> _topologies;
    private readonly IStreamConfig _streamConfig;
    public DefaultTopologyStreamWorker(IEnumerable<KafkaStream> apps, IEnumerable<IBuilder> topologies, IStreamConfig streamConfig)
    {
      _apps = apps;
      _topologies = topologies;
      _streamConfig = streamConfig;
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
      Task.Run(async () => await StartKafkaStreamsAsync(cancellationToken).ConfigureAwait(false), cancellationToken);

      return Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
      foreach (var kstreamApp in _apps)
      {
        kstreamApp.Dispose();
      }
      return Task.CompletedTask;
    }
    private async Task StartKafkaStreamsAsync(CancellationToken cancellationToken)
    {
      ///Set up topologies requirements
      foreach (var t in _topologies)
      {
        await t.Prerequisites(_streamConfig).ConfigureAwait(false);
      }
      ///Set up KStream apps requirements first
      foreach (var kstreamApp in _apps)
      {
        await kstreamApp.StartAsync(cancellationToken);
      }
    }
  }
}
