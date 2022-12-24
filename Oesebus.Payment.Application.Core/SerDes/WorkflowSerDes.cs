﻿using System.Text;
using System.Text.Json;
using Com.Evs.Phoenix.Workflow.Protobuf;
using Confluent.Kafka;
using Evs.Phoenix.Utils.Bus.Kafka.Protobuf;
using Streamiz.Kafka.Net.SerDes;

namespace Oesebus.EVS.KafkaService.Application.Core.SerDes;
public class WorkflowSerDes : AbstractSerDes<WorkflowEvent>
{
  public override WorkflowEvent Deserialize(byte[] data, SerializationContext context)
  {
    var msg = Encoding.UTF8.GetString(data);

    var serialized = PermissiveJsonParser.Parse<WorkflowEvent>(msg);

    return serialized;
  }
  public override byte[] Serialize(WorkflowEvent data, SerializationContext context) => JsonSerializer.SerializeToUtf8Bytes((data));
}
