using Confluent.Kafka;
using Streamiz.Kafka.Net.SerDes;

namespace Oesebus.EVS.KafkaService.Application.Core.SerDes;

public class XtBridgeEventJoinSerdes : AbstractSerDes<XtBridgeEventJoinSerdes>
{
  public override XtBridgeEventJoinSerdes Deserialize(byte[] data, SerializationContext context) => throw new System.NotImplementedException();
  public override byte[] Serialize(XtBridgeEventJoinSerdes data, SerializationContext context) => throw new System.NotImplementedException();
}
