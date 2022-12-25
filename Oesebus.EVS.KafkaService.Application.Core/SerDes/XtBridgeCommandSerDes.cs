using System;
using System.Text;
using System.Text.Json;
using Com.Evs.Pam.Service.Xtbridge.Api;
using Confluent.Kafka;
using Evs.Phoenix.Utils.Bus.Kafka.Protobuf;
using Streamiz.Kafka.Net.SerDes;

namespace Oesebus.EVS.KafkaService.Application.Core.SerDes;

public class XtBridgeCommandSerDes : AbstractSerDes<XtBridgeCommand>
{
  public override XtBridgeCommand Deserialize(byte[] data, SerializationContext context)
  {
    var msg = Encoding.UTF8.GetString(data);

    var serialized = PermissiveJsonParser.Parse<XtBridgeCommand>(msg);

    return serialized;
  }
  public override byte[] Serialize(XtBridgeCommand data, SerializationContext context) => JsonSerializer.SerializeToUtf8Bytes((data, typeof(XtBridgeCommand)));
}
