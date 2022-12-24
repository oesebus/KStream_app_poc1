using System;
using System.Text.Json;
using Com.Evs.Pam.Orchestrator.Xt.Media.Asset.Api;
using Com.Evs.Pam.Service.Recorder.Group.Api;
using Com.Evs.Pam.Service.Xt.Recorder.Api;
using Com.Evs.Pam.Service.Xtbridge.Api;
using Evs.Xtvia.Proto.Api.Mam;
using Microsoft.Extensions.DependencyInjection;
using Oesebus.EVS.KafkaService.Application.Core.Aggregates;
using Oesebus.EVS.KafkaService.Application.Core.Models;
using Oesebus.EVS.KafkaService.Application.Core.SerDes;
using Oesebus.Order.Infrastructure.Joiners;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Table;

namespace Evs.Phoenix.Utils.Orchestration.Saga.Persistence.KStream.Toplogies;

public static class SagaTopology
{
  public static Topology Create(IServiceCollection container, IStreamConfig config, bool describe)
  {
    StreamBuilder builder = new();
    KafkaStream stream;

    //var MA_StatesSagaStore = Materialized<string, ISagaState, Streamiz.Kafka.Net.State.IKeyValueStore<Streamiz.Kafka.Net.Crosscutting.Bytes, byte[]>>.Create();
    /////Materialized view that can be fetched / queried with Interactive queries and REST API
    //streamBuilder.Stream<string, ISagaState>("KStream_SagaState_source")
    // .ToTable(MA_StatesSagaStore, "MA_StatesSagaStore");
    //streamBuilder.Stream<string, ISagaLogData>("KStream_SagaLog_source");

    IKStream<string, XtBridgeEvent> KS0 = builder.Stream("XtBridgeEvent", new StringSerDes(), new XtBridgeEventSerDes());
    IKStream<string, XtMediaAssetOrchestratorCommand> KS1 = builder.Stream("XtMediaAssetOrchestratorCommand", new StringSerDes(), new XtMediaAssetOrchestratorCommandSerDes());
    IKStream<string, XtMediaAssetOrchestratorEvent> KS2 = builder.Stream("XtMediaAssetOrchestratorEvent", new StringSerDes(), new XtMediaAssetOrchestratorEventSerDes());
    IKStream<string, XtBridgeCommand> KS3 = builder.Stream("XtBridgeCommand", new StringSerDes(), new XtBridgeCommandSerDes());
    IKStream<string, RecorderGroupQueryCommand> KS4 = builder.Stream("RecorderGroupQueryCommand", new StringSerDes(), new RecorderGroupQueryCommandSerDes());
    IKStream<string, XtRecorderEvent> KS5 = builder.Stream("XtRecorderEvent", new StringSerDes(), new XtRecorderEventSerDes());
    IKStream<string, MamQueryCommand> KS6 = builder.Stream("MamQueryCommand_00000000-0000-0064-0146-000000364870", new StringSerDes(), new MamQueryCommandSerDes());

    //KS6.Filter((_, v) => !v.GetMediaAssets.Filters.Contains(new MediaAssetFilterProperty() { FilterNodeId = new() { Value = { "5b437246-7b76-6c4f-0000-000000000000" } } })).To("MamQueryCommandInvestigator");

    //var KS7 = KS6.Filter((_, v) => v.GetMediaAssets.Filters.Contains(new MediaAssetFilterProperty() { FilterNodeId = new() { Value = { "5b437246-7b76-6c4f-0000-000000000000" } } }));

    //KS7.Print(Printed<string, MamQueryCommand>.ToOut());

    KS0.Peek((k, v) => Console.WriteLine($"XyBridgeEvent =>  Key {k} ## Specific Case {v.SpecificCase}"));

    KS0.Foreach((k, v) => Console.WriteLine($" Key {k} ## Specific Case {v.SpecificCase}"));

    /// Select discussion ID as a key first and map values to a BridgeDiscussion model for all streams .

    var KS0Keyed = KS0.SelectKey((_, v) => v.Headers.DiscussionId).MapValues((v) => StoryBuilder.Map(v));
    var KS1Keyed = KS1.Filter((_, v) => v.SpecificCase is not XtMediaAssetOrchestratorCommand.SpecificOneofCase.None).SelectKey((_, v) => v.Headers.DiscussionId).MapValues((v) => StoryBuilder.Map(v));
    var KS2Keyed = KS2.Filter((_, v) => v.SpecificCase is not XtMediaAssetOrchestratorEvent.SpecificOneofCase.None).SelectKey((_, v) => v.Headers.DiscussionId).MapValues((v) => StoryBuilder.Map(v));
    var KS3Keyed = KS3.Filter((_, v) => v.SpecificCase is not XtBridgeCommand.SpecificOneofCase.None).SelectKey((_, v) => v.Headers.DiscussionId).MapValues((v) => StoryBuilder.Map(v));

    //var KS4Keyed = KS4.SelectKey((_, v) => v.Headers.DiscussionId).MapValues((v) => BridgeDiscussion.Map(v));

    KS3Keyed.Peek((k, v) => Console.WriteLine($" Key {k} ## Name {v.Name} Name {v.Domain}"));

    /// Hash(key) % nb Partitions
    /// Merge BridgeDiscussion streams togeteher to have a larger stream before aggregate operations on it
    var mergedRawDiscussion = KS0Keyed.Merge(KS1Keyed)
                                      .Merge(KS2Keyed)
                                      .Merge(KS3Keyed)
                                     /// Aggregate records and push it in a InMemory Materialized view (For quering purposes through API)
                                     .GroupByKey().Aggregate(() => new DiscussionSummary(), (_, v, old) => old.Aggregate(v),
                                      InMemory.As<string, DiscussionSummary>("mBridgeAggregateView").WithValueSerdes<DiscussionAggregatorSerDes>());

    //Join 3 streams in one stream based on key
    //var kS0JoinedKS1Stream = KS0Keyed.Join(KS1Keyed, (v0, v1) => new XtBridgeEventAndQueryEventValueMapper().Apply(v0, v1), JoinWindowOptions.Of(TimeSpan.FromSeconds(120)))
    //                                  .Join(KS2Keyed, (v1, v2) => new XtBridgeCommandValueMapper().Apply(v2, v1), JoinWindowOptions.Of(TimeSpan.FromSeconds(120)));

    /////Group the stream by key and aggregate values . The State store is materizalized in order to be queried.
    //var mBridgeAggregateView = kS0JoinedKS1Stream.GroupByKey().Aggregate(() => new DiscussionAggregator(), (k, v, old) => old.Aggregate(v),
    //    InMemory<string, DiscussionAggregator>.As("mBridgeAggregateView").WithValueSerdes<DiscussionAggregatorSerDes>());


    //mergedRawDiscussion.ToStream().Print(Printed<string, DiscussionSummary>.ToOut());

    ///Branch into 3 separate streams
    //var branchedStreams = KS0.Branch(ServerAdded, ServerUpdated, ServerRemoved);
    //var ServerAddedStream = branchedStreams[0];
    //var ServerUpdatedStream = branchedStreams[1];
    //var ServerRemovedStream = branchedStreams[2];

    //ServerAddedStream.Print(Printed<string, XtBridgeEvent>.ToOut());
    //ServerUpdatedStream.Print(Printed<string, XtBridgeEvent>.ToOut());
    //ServerRemovedStream.Print(Printed<string, XtBridgeEvent>.ToOut());

    //GroupBy discussion ID to prepare for discussion aggregations
    var groupedStreamByDiscussionId = KS0.GroupBy((_, v) => v.Headers.DiscussionId);

    var dag = builder.Build();

    if (describe)
      dag.Describe();

    stream = new KafkaStream(dag, config);

    container.AddSingleton(stream);
    container.AddSingleton(dag);

    return dag;
  }
}
