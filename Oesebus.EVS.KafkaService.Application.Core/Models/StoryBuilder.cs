using System;
using Com.Evs.Pam.Common.Api;
using Google.Protobuf;
using Google.Protobuf.Reflection;

namespace Oesebus.EVS.KafkaService.Application.Core.Models;

public class StoryBuilder: IMessage
{
  public DateTime Timestamp { get;  set; }
  public string Domain { get;  set; }
  public string Name { get;  set; }
  public string DiscussionId { get;  set; }
  public string Server { get;  set; }
  public string UniqueId { get;  set; }

  public string Source { get; set; }

  public MessageDescriptor Descriptor => null;

  public Headers Headers { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }

  public StoryBuilder WithTimeStamp(DateTime timestamp)
  {
    this.Timestamp = timestamp;
    return this;
  }

  public StoryBuilder WithSource(string source)
  {
    this.Source = source;
    return this;
  }

  public StoryBuilder WithDomain(string domain)
  {
    this.Domain = domain;
    return this;
  }
  public StoryBuilder WithName(string name)
  {
    this.Name = name;
    return this;
  }
  public StoryBuilder WithDiscussionId(string discussion)
  {
    this.DiscussionId = discussion;
    return this;
  }
  public StoryBuilder WithServer(string server)
  {
    this.Server = server;
    return this;
  }
  public StoryBuilder WithUniqueId(string requestid)
  {
    this.UniqueId = requestid;
    return this;
  }

  public static StoryBuilder Map(IHeaderMessage v)
  {
    return new StoryBuilder().WithDiscussionId(v.Headers.DiscussionId)
     .WithTimeStamp(v.Headers.Timestamp.ToDateTime())
     .WithServer(v.Headers.Source)
     .WithDomain(v.Headers.Domain)
     .WithUniqueId(v.Headers.RequestId)
     .WithSource(v.Headers.SiteOrigin)
     .WithName(v.Headers.Name);
  }

  public void MergeFrom(CodedInputStream input) => throw new NotImplementedException();
  public void WriteTo(CodedOutputStream output) => throw new NotImplementedException();
  public int CalculateSize() => throw new NotImplementedException();
}
