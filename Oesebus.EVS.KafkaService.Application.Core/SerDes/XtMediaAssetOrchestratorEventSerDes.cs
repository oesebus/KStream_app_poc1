using System.Text;
using System.Text.Json;
using Com.Evs.Pam.Orchestrator.Xt.Media.Asset.Api;
using Confluent.Kafka;
using Evs.Phoenix.Utils.Bus.Kafka.Protobuf;
using Streamiz.Kafka.Net.SerDes;

namespace Oesebus.EVS.KafkaService.Application.Core.SerDes;

public class XtMediaAssetOrchestratorEventSerDes : AbstractSerDes<XtMediaAssetOrchestratorEvent>
{
  public override XtMediaAssetOrchestratorEvent Deserialize(byte[] data, SerializationContext context)
  {
    var msg = Encoding.UTF8.GetString(data);

    var serialized = PermissiveJsonParser.Parse<XtMediaAssetOrchestratorEvent>(msg);

    return serialized;
  }
  public override byte[] Serialize(XtMediaAssetOrchestratorEvent data, SerializationContext context) => JsonSerializer.SerializeToUtf8Bytes((data, typeof(XtMediaAssetOrchestratorEvent)));
}
