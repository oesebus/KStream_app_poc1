using System.Text;
using System.Text.Json;
using Confluent.Kafka;
using Evs.Phoenix.Utils.Bus.Kafka.Protobuf;
using Oesebus.EVS.KafkaService.Application.Core.Aggregates;
using Streamiz.Kafka.Net.SerDes;

namespace Oesebus.EVS.KafkaService.Application.Core.SerDes;

public class DiscussionAggregatorSerDes : AbstractSerDes<DiscussionSummary>
{
  public override DiscussionSummary Deserialize(byte[] data, SerializationContext context)
  {
    var msg = Encoding.UTF8.GetString(data);

    var serialized = PermissiveJsonParser.Parse<DiscussionSummary>(msg);

    return serialized;
  }
  public override byte[] Serialize(DiscussionSummary data, SerializationContext context) => JsonSerializer.SerializeToUtf8Bytes((data, typeof(DiscussionSummary)));
}