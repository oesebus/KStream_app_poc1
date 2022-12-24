using System.IO;
using System.Text;
using System.Text.Json;
using Com.Evs.Pam.Service.Xtbridge.Api;
using Confluent.Kafka;
using Evs.Phoenix.Utils.Bus.Kafka.Protobuf;
using Streamiz.Kafka.Net.SerDes;

namespace Oesebus.EVS.KafkaService.Application.Core.SerDes;

public class XtBridgeQueryCommandSerDes : AbstractSerDes<XtBridgeQueryCommand>
{
  public override XtBridgeQueryCommand Deserialize(byte[] data, SerializationContext context)
  {
    var msg = Encoding.UTF8.GetString(data);

    var serialized = PermissiveJsonParser.Parse<XtBridgeQueryCommand>(msg);

    return serialized;
  }
  public override byte[] Serialize(XtBridgeQueryCommand data, SerializationContext context) => JsonSerializer.SerializeToUtf8Bytes((data, typeof(XtBridgeQueryCommand)));
}
