using Com.Evs.Pam.Service.Xtbridge.Api;
using Oesebus.EVS.KafkaService.Application.Core.Models;
using Oesebus.EVS.KafkaService.Application.Core.SerDes;
using Streamiz.Kafka.Net.Stream;

namespace Oesebus.Order.Infrastructure.Joiners
{
  public class XtBridgeEventAndQueryEventValueMapper : IValueJoiner<XtBridgeEvent, XtBridgeQueryCommand, XtBridgeEventJoinerResult>
  {
    public XtBridgeEventJoinerResult Apply(XtBridgeEvent value1, XtBridgeQueryCommand value2) => new();
  }
}