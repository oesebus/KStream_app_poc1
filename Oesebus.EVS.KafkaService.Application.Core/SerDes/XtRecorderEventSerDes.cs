using System.Text;
using System.Text.Json;
using Com.Evs.Pam.Service.Xt.Recorder.Api;
using Confluent.Kafka;
using Evs.Phoenix.Utils.Bus.Kafka.Protobuf;
using Streamiz.Kafka.Net.SerDes;

namespace Oesebus.EVS.KafkaService.Application.Core.SerDes;

public class XtRecorderEventSerDes : AbstractSerDes<XtRecorderEvent>
{
  public override XtRecorderEvent Deserialize(byte[] data, SerializationContext context)
  {
    var msg = Encoding.UTF8.GetString(data);

    var serialized = PermissiveJsonParser.Parse<XtRecorderEvent>(msg);

    return serialized;  
  }
  public override byte[] Serialize(XtRecorderEvent data, SerializationContext context) => JsonSerializer.SerializeToUtf8Bytes((data, typeof(XtRecorderEvent)));
}
