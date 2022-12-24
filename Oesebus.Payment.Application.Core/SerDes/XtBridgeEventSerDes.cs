using System.Diagnostics;
using System.IO;
using System.Text;
using System.Text.Json;
using Com.Evs.Pam.Service.Xtbridge.Api;
using Confluent.Kafka;
using Evs.Phoenix.Utils.Bus.Kafka.Protobuf;
using Streamiz.Kafka.Net.SerDes;

namespace Oesebus.EVS.KafkaService.Application.Core.SerDes;

public class XtBridgeEventSerDes : AbstractSerDes<XtBridgeEvent>
{
  public override XtBridgeEvent Deserialize(byte[] data, SerializationContext context)
  {
    Debug.WriteLine("xtbridgeEvent Serdes : " + data?.ToString());

    if (data is null)
      return new XtBridgeEvent();

    var msg = Encoding.UTF8.GetString(data);

    var serialized = PermissiveJsonParser.Parse<XtBridgeEvent>(msg);

    return serialized;
  }
  public override byte[] Serialize(XtBridgeEvent data, SerializationContext context) => JsonSerializer.SerializeToUtf8Bytes((data, typeof(XtBridgeEvent)));
}
