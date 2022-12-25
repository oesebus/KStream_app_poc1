using Com.Evs.Pam.Common.Api;
using Com.Evs.Pam.Orchestrator.Xt.Media.Asset.Api;
using Com.Evs.Pam.Service.Xtbridge.Api;
using Com.Evs.Phoenix.Workflow.Protobuf;
using Confluent.Kafka.Admin;
using Confluent.Kafka;
using Oesebus.EVS.KafkaService.Application.Core.Aggregates;
using Oesebus.EVS.KafkaService.Application.Core.Models;
using Oesebus.EVS.KafkaService.Application.Core.SerDes;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Stream;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Streamiz.Kafka.Net.Table;
using Oesebus.EVS.KafkaService.Application.Core.Interfaces;

namespace Oesebus.Order.Infrastructure.Services
{
  public class EvsDicussionTopologyBuilder : IBuilder
  {
    public Topology Build()
    {
      StreamBuilder builder = new();

      #region "Predicates"
      Func<string, XtBridgeEvent, bool> ServerAdded = (_, v) => v.SpecificCase == XtBridgeEvent.SpecificOneofCase.ServerAdded;
      Func<string, XtBridgeEvent, bool> ServerUpdated = (_, v) => v.SpecificCase == XtBridgeEvent.SpecificOneofCase.ServerUpdated;
      Func<string, XtBridgeEvent, bool> ServerRemoved = (_, v) => v.SpecificCase == XtBridgeEvent.SpecificOneofCase.ServerRemoved;
      #endregion

      ///Materialize some topics ....
      IKStream<string, XtBridgeEvent> KS0 = builder.Stream("XtBridgeEvent", new StringSerDes(), new XtBridgeEventSerDes());
      IKStream<string, XtMediaAssetOrchestratorCommand> KS1 = builder.Stream("XtMediaAssetOrchestratorCommand", new StringSerDes(), new XtMediaAssetOrchestratorCommandSerDes());
      IKStream<string, XtMediaAssetOrchestratorEvent> KS2 = builder.Stream("XtMediaAssetOrchestratorEvent", new StringSerDes(), new XtMediaAssetOrchestratorEventSerDes());
      IKStream<string, XtBridgeCommand> KS3 = builder.Stream("XtBridgeCommand", new StringSerDes(), new XtBridgeCommandSerDes());
      IKStream<string, WorkflowEvent> KS4 = builder.Stream("WorkflowEvent", new StringSerDes(), new WorkflowSerDes());

      ///Filter based on a predicate
      var KS2Keyed = KS2.Filter((_, v) => v.SpecificCase is not XtMediaAssetOrchestratorEvent.SpecificOneofCase.MediaAssetCreationFailed).MapValues((v) => StoryBuilder.Map(v));

      ///MapValues operator will change the schema from source topic (without changing keys so no data redistribution required)
      //var mappedKS2Stream = KS2Keyed.MapValues((v) => StoryBuilder.Map(v));

      /////Branch into 3 separate streams based on predicate
      var branchedStreams = KS0.Branch(ServerAdded, ServerUpdated, ServerRemoved);

      var ServerAddedStream = branchedStreams[0];
      var ServerUpdatedStream = branchedStreams[1];
      var ServerRemovedStream = branchedStreams[2];

      ///Sink Streams to destination topic
      //mappedKS2Stream.To("destinationTopicOrchestratorEvent");
      ServerAddedStream.To("ServerAddedStreamTopic");
      ServerAddedStream.To("ServerUpdatedStreamTopic");
      ServerAddedStream.To("ServerRemovedStreamTopic");

      ///Select discussion ID as a key first and map values to a BridgeDiscussion model for all streams .
      var KS0Keyed = KS0.Filter((_, v) => v.SpecificCase is not XtBridgeEvent.SpecificOneofCase.None).MapValues((v) => StoryBuilder.Map(v));

      KS0Keyed.Print(Printed<string, StoryBuilder>.ToOut());

      var KS1Keyed = KS1.Filter((_, v) => v.SpecificCase is not XtMediaAssetOrchestratorCommand.SpecificOneofCase.None).MapValues((v) => StoryBuilder.Map(v));

      KS1Keyed.Print(Printed<string, StoryBuilder>.ToOut());

      //.Map((k, v) => new StoryBuilderKeyValueMapper<XtMediaAssetOrchestratorEvent>().Apply(k, v));

      var KS3Keyed = KS3.Filter((_, v) => v.SpecificCase is not XtBridgeCommand.SpecificOneofCase.None).SelectKey((_, v) => v.Headers.DiscussionId).MapValues((v) => StoryBuilder.Map(v));

      //KS3Keyed.Print(Printed<string, StoryBuilder>.ToOut());

      var KS4Keyed = KS4.SelectKey((_, v) => v.Headers.DiscussionId).MapValues((v) => StoryBuilder.Map(v));

      /// Merge BridgeDiscussion streams togeteher to have a larger stream before aggregate operations on it
      //var mergedRawDiscussion = KS0Keyed.Merge(KS1Keyed)
      //                                  .Merge(KS2Keyed)
      //                                  .Merge(KS3Keyed)
      //                                 /// Aggregate records and push it in a InMemory Materialized view (For quering purposes through API)
      //                                 .GroupByKey().Aggregate(() => new DiscussionSummary(), (_, v, old) => old.Aggregate(v),
      //                                  RocksDb.As<string, DiscussionSummary>("mBridgeAggregateView").WithValueSerdes<DiscussionAggregatorSerDes>());

      ///Hash(key) % nb Partitions
      ///Merge BridgeDiscussion streams togeteher to have a larger stream before aggregate operations on it
      //KS0Keyed.Merge(KS1Keyed)
      //        .Merge(KS2Keyed)
      //        .GroupByKey<StringSerDes, StorySerdes>()
      //        .Aggregate(() => new DiscussionSummary(), (_, v, old) => old.Aggregate(v))
      //        .ToStream()
      //        //.Repartition(Repartitioned<string, StoryBuilder>.NumberOfPartitions(6)
      //        //     .WithKeySerdes(new StringSerDes()).WithValueSerdes(new StorySerdes()))
      //        .To<StringSerDes, DiscussionAggregatorSerDes>("stories-viewtoplogy");

      KS2.Filter((_, v) => v.SpecificCase == XtMediaAssetOrchestratorEvent.SpecificOneofCase.MediaAssetReferencingCompleted
      || v.SpecificCase == XtMediaAssetOrchestratorEvent.SpecificOneofCase.MediaAssetReferencingStarted)
        .Print(Printed<string, XtMediaAssetOrchestratorEvent>.ToOut());

      //.Merge(KS3Keyed)
      //.Merge(KS4Keyed)
      //.Repartition(Repartitioned<string, StoryBuilder>.NumberOfPartitions(2)
      //     .WithKeySerdes(new StringSerDes()).WithValueSerdes(new StorySerdes()))
      //.GroupByKey<StringSerDes, StorySerdes>()
      ////.WindowedBy(TumblingWindowOptions.Of(2000))
      //.Aggregate(() => new DiscussionSummary(), (_, v, old) => old.Aggregate(v));

      //InMemory<string, DiscussionSummary>.As("mBridgeAggregateView").WithValueSerdes<DiscussionAggregatorSerDes>());
      //.Count(InMemory<string, long>.As("DiscussionCountState").WithValueSerdes<StorySerdes>());


      //mergedRawDiscussionKTable.ToStream().Peek((k, v) => Console.WriteLine($" Key {k} | Count : {v}"));

      //                                   /// Aggregate records and push it in a InMemory Materialized view (For quering purposes through API)
      // .GroupByKey<StringSerDes, StorySerdes>().Aggregate(() => new DiscussionSummary(), (_, v, old) => old.Aggregate(v),
      //InMemory<string, DiscussionSummary>.As("mBridgeAggregateView").WithValueSerdes<DiscussionAggregatorSerDes>());

      ///Join 3 streams in one stream based on key
      //var kS0JoinedKS1Stream = KS0Keyed.Join(KS1Keyed, (v0, v1) => new XtBridgeEventAndQueryEventValueMapper().Apply(v0, v1), JoinWindowOptions.Of(TimeSpan.FromSeconds(120)))
      //                                  .Join(KS2Keyed, (v1, v2) => new XtBridgeCommandValueMapper().Apply(v2, v1), JoinWindowOptions.Of(TimeSpan.FromSeconds(120)));

      /////Group the stream by key and aggregate values . The State store is materizalized in order to be queried.
      //var mBridgeAggregateView = kS0JoinedKS1Stream.GroupByKey().Aggregate(() => new DiscussionAggregator(), (k, v, old) => old.Aggregate(v),
      //    InMemory<string, DiscussionAggregator>.As("mBridgeAggregateView").WithValueSerdes<DiscussionAggregatorSerDes>());

      //mergedRawDiscussion.ToStream().Print(Printed<string, DiscussionSummary>.ToOut());

      ///Build topology

      var tp = builder.Build();
      tp.Describe();
      return tp;
    }
    public async Task Prerequisites(IStreamConfig streamConfig)
    {
      using (var adminClient = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = streamConfig.BootstrapServers }).Build())
      {
        try
        {
          var topicsInfo = new TopicSpecification[] {
                    new TopicSpecification { Name = "ServerAddedStreamTopic", ReplicationFactor = 1, NumPartitions = 1 },
                    new TopicSpecification { Name = "ServerUpdatedStreamTopic", ReplicationFactor = 1, NumPartitions = 1 },
                    new TopicSpecification { Name = "ServerRemovedStreamTopic", ReplicationFactor = 1, NumPartitions = 1 }
          };

          await adminClient.CreateTopicsAsync(topicsInfo);
        }
        catch (CreateTopicsException e)
        {
          Console.WriteLine($"An error occured creating topic {e.Results[0].Topic}: {e.Results[0].Error.Reason}");
        }
      }
    }
  }
}
