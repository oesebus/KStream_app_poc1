using System;
using System.Text;
using System.Text.Json;
using Com.Evs.Pam.Service.Recorder.Group.Api;
using Confluent.Kafka;
using Evs.Phoenix.Utils.Bus.Kafka.Protobuf;
using Streamiz.Kafka.Net.SerDes;

namespace Oesebus.EVS.KafkaService.Application.Core.SerDes;

public class RecorderGroupQueryCommandSerDes : AbstractSerDes<RecorderGroupQueryCommand>
{
  public override RecorderGroupQueryCommand Deserialize(byte[] data, SerializationContext context)
  {
    var msg = Encoding.UTF8.GetString(data);

    var serialized = PermissiveJsonParser.Parse<RecorderGroupQueryCommand>(msg);

    return serialized;
  }
  public override byte[] Serialize(RecorderGroupQueryCommand data, SerializationContext context)
  {
    return JsonSerializer.SerializeToUtf8Bytes((data, typeof(RecorderGroupQueryCommand)));
  }
}