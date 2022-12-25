using System;
using System.Collections.Generic;
using Google.Protobuf;
using Google.Protobuf.Reflection;
using Oesebus.EVS.KafkaService.Application.Core.Models;

namespace Oesebus.EVS.KafkaService.Application.Core.Aggregates;

public class DiscussionDescriptor : IDescriptor
{
  public string FullName => "DiscussionDescriptor";

  public FileDescriptor File => (FileDescriptor)new object();
  public string Name => "DiscussionSummary";
}
public class DiscussionSummary : IMessage
{
  public DateTime IniializerDate { get; set; }
  public int numberOfEvents { get; set; }
  public string DiscussionId { get; set; }
  public DateTime SagaStartedDate { get; set; }
  public DateTime SagaCompletedDate { get; set; }
  public TimeSpan SagaDuration { get; set; }
  public string SagaProcessingLatency { get; set; }
  public bool IsCompleted { get; set; }
  public Dictionary<string, StoryBuilder> Stories { get; set; } = new();
  public Dictionary<string, StoryBuilder> PotentialDuplicates { get; set; } = new();
  public MessageDescriptor Descriptor => null;

  public DiscussionSummary Aggregate(StoryBuilder v)
  {
    SagaDuration = SagaDuration.Add(v.Timestamp.TimeOfDay);

    if (SagaCompletedDate < v.Timestamp.Date)
      SagaCompletedDate = v.Timestamp.Date;

    Stories.Add(v.UniqueId, v);

    if (Stories.ContainsKey(v.UniqueId))
      PotentialDuplicates.Add(v.UniqueId, v);

    if (String.IsNullOrEmpty(DiscussionId))
      DiscussionId = v.DiscussionId;

    this.numberOfEvents++;

    return this;
  }

  public void MergeFrom(CodedInputStream input)
  {

  }
  public void WriteTo(CodedOutputStream output)
  {
  }
  public int CalculateSize() => 0;

  public DiscussionSummary()
  {
    IniializerDate = DateTime.UtcNow;
    IsCompleted = false;
    SagaDuration = TimeSpan.Zero;
  }
  public enum Status
  {
    Started,
    Completed,
    Processing,
    Failed
  }
}
