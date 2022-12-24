using System;
using System.Text;
using System.Text.Json;
using Confluent.Kafka;
using Evs.Phoenix.Utils.Bus.Kafka.Protobuf;
using Evs.Xtvia.Proto.Api.Mam;
using Streamiz.Kafka.Net.SerDes;

namespace Oesebus.EVS.KafkaService.Application.Core.SerDes;

public class MamQueryCommandSerDes : AbstractSerDes<MamQueryCommand>
{
  public override MamQueryCommand Deserialize(byte[] data, SerializationContext context)
  {
    var msg = Encoding.UTF8.GetString(data);

    var serialized = PermissiveJsonParser.Parse<MamQueryCommand>(msg);

    return serialized;
  }
  public override byte[] Serialize(MamQueryCommand data, SerializationContext context) =>  JsonSerializer.SerializeToUtf8Bytes((data, typeof(MamQueryCommand)));
}
