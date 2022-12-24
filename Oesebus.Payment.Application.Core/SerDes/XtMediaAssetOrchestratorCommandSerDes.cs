using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using Com.Evs.Pam.Orchestrator.Xt.Media.Asset.Api;
using Confluent.Kafka;
using Evs.Phoenix.Utils.Bus.Kafka.Protobuf;
using Streamiz.Kafka.Net.SerDes;

namespace Oesebus.EVS.KafkaService.Application.Core.SerDes;

public class XtMediaAssetOrchestratorCommandSerDes : AbstractSerDes<XtMediaAssetOrchestratorCommand>
{
  public override XtMediaAssetOrchestratorCommand Deserialize(byte[] data, SerializationContext context)
  {
    var msg = Encoding.UTF8.GetString(data);

    var serialized = PermissiveJsonParser.Parse<XtMediaAssetOrchestratorCommand>(msg);

    return serialized;
  }
  public override byte[] Serialize(XtMediaAssetOrchestratorCommand data, SerializationContext context) => JsonSerializer.SerializeToUtf8Bytes((data, typeof(XtMediaAssetOrchestratorCommand)));
}
