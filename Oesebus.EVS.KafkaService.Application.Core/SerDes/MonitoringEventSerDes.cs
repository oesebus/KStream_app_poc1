using System.Text;
using System.Text.Json;
using Com.Evs.Pam.Service.Monitoringbridge.Api;
using Confluent.Kafka;
using Evs.Phoenix.Utils.Bus.Kafka.Protobuf;
using Streamiz.Kafka.Net.SerDes;

namespace Oesebus.EVS.KafkaService.Application.Core.SerDes;
public class MonitoringEventSerDes : AbstractSerDes<MonitoringEvent>
{
  public override MonitoringEvent Deserialize(byte[] data, SerializationContext context)
  {
    var msg = Encoding.UTF8.GetString(data);

    var serialized = PermissiveJsonParser.Parse<MonitoringEvent>(msg);

    return serialized;
  }
  public override byte[] Serialize(MonitoringEvent data, SerializationContext context) => JsonSerializer.SerializeToUtf8Bytes((data));

}
