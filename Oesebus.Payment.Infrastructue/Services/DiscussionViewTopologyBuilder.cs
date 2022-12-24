using Com.Evs.Pam.Common.Api;
using Com.Evs.Pam.Orchestrator.Xt.Media.Asset.Api;
using Com.Evs.Pam.Service.Xtbridge.Api;
using Com.Evs.Phoenix.Workflow.Protobuf;
using Confluent.Kafka.Admin;
using Confluent.Kafka;
using Oesebus.EVS.KafkaService.Application.Core.Aggregates;
using Oesebus.EVS.KafkaService.Application.Core.Models;
using Oesebus.EVS.KafkaService.Application.Core.SerDes;
using Oesebus.Order.Application.Core.Interfaces;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Stream;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Streamiz.Kafka.Net.Table;
using Microsoft.Extensions.DependencyInjection;

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

      //private readonly IValueJoiner<InvoiceSerdes, User, UserSerDes> _UserOrderValueJoiner;
      //private readonly Materialized<string, InvoiceSerdes, Streamiz.Kafka.Net.State.IKeyValueStore<Streamiz.Kafka.Net.Crosscutting.Bytes, byte[]>> _materialized;

      //IKStream<string, XtRecorderEvent> KS5 = builder.Stream("XtRecorderEvent", new StringSerDes(), new XtRecorderEventSerDes());
      //IKStream<string, MamQueryCommand> KS6 = builder.Stream("MamQueryCommand_00000000-0000-0064-0146-000000364870", new StringSerDes(), new MamQueryCommandSerDes());

      //KS6.Filter((_, v) => !v.GetMediaAssets.Filters.Contains(new MediaAssetFilterProperty() { FilterNodeId = new() { Value = { "5b437246-7b76-6c4f-0000-000000000000" } } })).To("MamQueryCommandInvestigator");

      //var KS7 = KS6.Filter((_, v) => v.GetMediaAssets.Filters.Contains(new MediaAssetFilterProperty() { FilterNodeId = new() { Value = { "5b437246-7b76-6c4f-0000-000000000000" } } }));

      //KS7.Print(Printed<string, MamQueryCommand>.ToOut());

      //KS0.Peek((k, v) => Console.WriteLine($"XyBridgeEvent =>  Key {k} ## Specific Case {v.SpecificCase}"));

      //KS0.Foreach((k, v) => Console.WriteLine($" Key {k} ## Specific Case {v.SpecificCase}"));

      ///Select discussion ID as a key first and map values to a BridgeDiscussion model for all streams .
      var KS0Keyed = KS0.Filter((_, v) => v.SpecificCase is not XtBridgeEvent.SpecificOneofCase.None).MapValues((v) => StoryBuilder.Map(v));

      KS0Keyed.Print(Printed<string, StoryBuilder>.ToOut());

      var KS1Keyed = KS1.Filter((_, v) => v.SpecificCase is not XtMediaAssetOrchestratorCommand.SpecificOneofCase.None).MapValues((v) => StoryBuilder.Map(v));

      KS1Keyed.Print(Printed<string, StoryBuilder>.ToOut());

      //.Map((k, v) => new StoryBuilderKeyValueMapper<XtMediaAssetOrchestratorEvent>().Apply(k, v));

      var KS3Keyed = KS3.Filter((_, v) => v.SpecificCase is not XtBridgeCommand.SpecificOneofCase.None).SelectKey((_, v) => v.Headers.DiscussionId).MapValues((v) => StoryBuilder.Map(v));

      KS3Keyed.Print(Printed<string, StoryBuilder>.ToOut());

      var KS4Keyed = KS4.SelectKey((_, v) => v.Headers.DiscussionId).MapValues((v) => StoryBuilder.Map(v));

      /// Merge BridgeDiscussion streams togeteher to have a larger stream before aggregate operations on it
      var mergedRawDiscussion = KS0Keyed.Merge(KS1Keyed)
                                        .Merge(KS2Keyed)
                                        .Merge(KS3Keyed)
                                       /// Aggregate records and push it in a InMemory Materialized view (For quering purposes through API)
                                       .GroupByKey().Aggregate(() => new DiscussionSummary(), (_, v, old) => old.Aggregate(v),
                                        RocksDb.As<string, DiscussionSummary>("mBridgeAggregateView").WithValueSerdes<DiscussionAggregatorSerDes>());

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

      //ServerAddedStream.Print(Printed<string, XtBridgeEvent>.ToOut());
      //ServerUpdatedStream.Print(Printed<string, XtBridgeEvent>.ToOut());
      //ServerRemovedStream.Print(Printed<string, XtBridgeEvent>.ToOut());

      ///GroupBy discussion ID to prepare for discussion aggregations
      //var groupedStreamByDiscussionId = KS0.GroupBy((_, v) => v.Headers.DiscussionId);

      /// Aggregate Point of sales with orders to calculate revenues per product
      //var aggStream = groupedStream
      //                    .WindowedBy(TumblingWindowOptions.Of(2000))
      //                    .Aggregate();

      //IKStream<string, User> KS1 = builder.Stream("User", new StringSerDes(), new StringSerDes())
      //   .MapValues((_, v) => JsonSerializer.Deserialize<User>(v)); /// Parse json string into POSInvoice type

      // Stream data will be compacted by key(Events/ facts-- > Updates)
      //var InMemoryPosStore = KS0.ToTable();

      //    ///Materialized View example
      //var InvoiceTableView = KS0.ToTable(Materialized<string, Invoice,
      //  Streamiz.Kafka.Net.State.IKeyValueStore<Streamiz.Kafka.Net.Crosscutting.Bytes, byte[]>>.Create(), "InvoiceStateStore");

      //    var UserTableView = KS1.ToTable(Materialized<string, User,
      //Streamiz.Kafka.Net.State.IKeyValueStore<Streamiz.Kafka.Net.Crosscutting.Bytes, byte[]>>.Create(), "UserStateStore");

      ///Left join Invoice table with User table
      //var RevenueKtable = InvoiceTableView.LeftJoin(UserTableView, (v1, v2) => _UserOrderValueJoiner.Apply(v1, v2));

      ///Requirement 1 : we filter out those records whose delivery type is 'HOME-DELIVERY'
      ///and then push all of those specific MessageRecords to the new topic 
      //  KS0.Filter((_, v) => v.Delivery == DeliveryType.HomeDelivery).To<StringSerDes, InvoiceSerdes>("Shipment");

      ///Print data to view logs in console output.
      //KS0.Print(Printed<string, Invoice>.ToOut());

      ///Requirement 2 : We filter out those records whose customer type is ‘PRIME’ and then push
      //KS0.Filter((_, v) => v.customerType == CustomerType.Prime)
      //  .MapValues(invoice => OrderCreated.MapEvent(invoice)).To("PrimeOrderEvent", "Prime");

      ///Requirement 3: Select All invoices , mask all card number
      //KS0.MapValues(async invoice => await _auditService.AnonymizeCard(invoice)).To("NotificationTopic");

      /// Store in SQL Database Premium customer sales
      //KS0.Filter((_, v) => v.customerType == CustomerType.Classic)
      //    .Foreach((_, v) => CustomerService.Store(v));

      //KS0.SelectKey(new InvoiceValueMapper(), "Select Key processor").To("POSKEYED");

      ///Build topology
      return builder.Build();

      //Display topology in console
      //foreach (var item in _topology.Describe().SubTopologies)
      //{
      //  Console.WriteLine("Sub topoligy ID : " + item.Id);
      //  foreach (var node in item.Nodes)
      //  {
      //    Console.WriteLine("Node ID : " + node.Name);
      //  }
      //}
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

          //ServerAddedStream.To("ServerAddedStreamTopic");
          //ServerAddedStream.To("ServerUpdatedStreamTopic");
          //ServerAddedStream.To("ServerRemovedStreamTopic");

          await adminClient.CreateTopicsAsync(topicsInfo);
        }
        catch (CreateTopicsException e)
        {
          Console.WriteLine($"An error occured creating topic {e.Results[0].Topic}: {e.Results[0].Error.Reason}");
        }
      }
    }




    private class StoryBuilderKeyValueMapper<T> : IKeyValueMapper<string, T, KeyValuePair<string, StoryBuilder>> where T : IHeaderMessage, new()
    {
      public KeyValuePair<string, StoryBuilder> Apply(string key, T value)
      {
        return new KeyValuePair<string, StoryBuilder>(key, new()
        {
          DiscussionId = key,
          Domain = value.Headers.Domain,
          Timestamp = value.Headers.Timestamp.ToDateTime(),
          Server = value.Headers.SiteOrigin
        });
      }
    }
  }
}
