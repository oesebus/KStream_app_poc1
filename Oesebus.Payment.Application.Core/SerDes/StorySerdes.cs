using System.Text;
using System.Text.Json;
using Confluent.Kafka;
using Evs.Phoenix.Utils.Bus.Kafka.Protobuf;
using Oesebus.EVS.KafkaService.Application.Core.Models;
using Streamiz.Kafka.Net.SerDes;

namespace Oesebus.EVS.KafkaService.Application.Core.SerDes;

public class StorySerdes : AbstractSerDes<StoryBuilder>
{
  public override StoryBuilder Deserialize(byte[] data, SerializationContext context)
  {
    var msg = Encoding.UTF8.GetString(data);

    var serialized = PermissiveJsonParser.Parse<StoryBuilder>(msg);

    return serialized;
  }
  public override byte[] Serialize(StoryBuilder data, SerializationContext context) => JsonSerializer.SerializeToUtf8Bytes((data));
}
