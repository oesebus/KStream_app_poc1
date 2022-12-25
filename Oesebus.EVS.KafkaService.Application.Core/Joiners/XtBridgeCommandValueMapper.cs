using Com.Evs.Pam.Service.Xtbridge.Api;
using Oesebus.EVS.KafkaService.Application.Core.Models;
using Streamiz.Kafka.Net.Stream;

namespace Oesebus.EVS.KafkaService.Application.Core.Joiners;

public class XtBridgeCommandValueMapper : IValueJoiner<XtBridgeCommand, XtBridgeEventJoinerResult, StoryBuilder>
{
  public StoryBuilder Apply(XtBridgeCommand value1, XtBridgeEventJoinerResult value2)
  {
    return new StoryBuilder().WithDiscussionId(value1.Headers.DiscussionId)
                                 .WithTimeStamp(value1.Headers.Timestamp.ToDateTime())
                                 .WithDomain(value1.Headers.Domain)
                                 .WithName(value1.Headers.Name);
  }
}
