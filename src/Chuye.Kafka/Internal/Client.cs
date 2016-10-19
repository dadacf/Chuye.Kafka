using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Chuye.Kafka.Protocol;

namespace Chuye.Kafka.Internal {
    public class Client {
        private const String TermPattern = "[a-zA-Z][a-zA-Z0-9_-]*";
        private const String __consumer_offsets = "__consumer_offsets";
        private readonly ExistingBrokerDispatcher _existingBrokerDispatcher;
        //todo: chicken-and-egg problem
        private /*readonly*/ TopicBrokerDispatcher _topicBrokerDispatcher;
        private readonly Option _option;

        public event EventHandler<RequestSubmittingEventArgs> RequestSubmitting;

        public Client(Option option)
            : this(option, new ExistingBrokerDispatcher(option.BrokerUris)) {
        }

        internal Client(Option option, ExistingBrokerDispatcher existingBrokerDispatcher) {
            _option = option;
            _existingBrokerDispatcher = existingBrokerDispatcher;
            _topicBrokerDispatcher = new TopicBrokerDispatcher(this);
        }

        //todo: chicken-and-egg problem
        internal void ReplaceDispatcher(TopicBrokerDispatcher topicBrokerDispatcher) {
            _topicBrokerDispatcher = topicBrokerDispatcher;
        }


        private void EnsureLegalTopicSpelling(String topic) {
            if (topic == null) {
                throw new ArgumentNullException("topic");
            }
            if (!Regex.IsMatch(topic, TermPattern)) {
                throw new ArgumentOutOfRangeException("topic",
                    String.Format("Regex '{0}' test fail", TermPattern));
            }
        }

        private Response SubmitRequest(Uri uri, Request request) {
            var @event = new RequestSubmittingEventArgs(uri, request);
            OnRequestSubmitting(@event);
            var connectionFactory = _option.GetConnectionFactory();
            using (var connection = connectionFactory.Connect(@event.Uri)) {
                return connection.Submit(@event.Request);
            }
        }

        private void OnRequestSubmitting(RequestSubmittingEventArgs @event) {
            RequestSubmitting?.Invoke(this, @event);
        }

        public MetadataResponse Metadata(params String[] topics) {
            if (topics == null) {
                topics = new String[0];
            }
            foreach (var topic in topics) {
                EnsureLegalTopicSpelling(topic);
            }

            var brokerUri = _existingBrokerDispatcher.FreeSelect();
            var request = new MetadataRequest(topics);
            var response = (MetadataResponse)SubmitRequest(brokerUri, request);
            response.TopicMetadatas = response.TopicMetadatas
                .Where(x => x.TopicName != __consumer_offsets).ToArray();
            return response;
        }

        public Int64 Produce(String topic, Int32 partition, IList<Message> messages) {
            EnsureLegalTopicSpelling(topic);
            var broker = _topicBrokerDispatcher.Select(topic, partition);
            var request = new ProduceRequest(topic, partition, messages);
            var response = (ProduceResponse)SubmitRequest(broker.ToUri(), request);
            response.TryThrowFirstErrorOccured();
            return response.TopicPartitions[0].Details[0].Offset;
        }

        public Int64 Produce(String topic, Int32 partition, params String[] messages) {
            return Produce(topic, partition, messages.Select(x => (Message)x).ToArray());
        }

        public IEnumerable<OffsetMessage> Fetch(String topic, Int32 partition, Int64 fetchOffset) {
            EnsureLegalTopicSpelling(topic);
            var broker = _topicBrokerDispatcher.Select(topic, partition);
            var request = new FetchRequest(topic, partition, fetchOffset);
            var response = (FetchResponse)SubmitRequest(broker.ToUri(), request);
            response.TryThrowFirstErrorOccured();
            return response.TopicPartitions.SelectMany(x => x.MessageBodys)
                .SelectMany(x => x.MessageSet.Items)
                .Where(x => x.Offset >= fetchOffset)
                .Select(msg => new OffsetMessage(msg.Offset, msg.Message.Key, msg.Message.Value));
        }

        public Int64 Offset(String topic, Int32 partition, OffsetOption option) {
            EnsureLegalTopicSpelling(topic);
            var broker = _topicBrokerDispatcher.Select(topic, partition);
            var request = new OffsetRequest(topic, new[] { partition }, option);
            var response = (OffsetResponse)SubmitRequest(broker.ToUri(), request);
            var errors = response.TopicPartitions
                .SelectMany(r => r.PartitionOffsets)
                .Where(x => x.ErrorCode != ErrorCode.NoError)
                .ToList();
            if (errors.Count > 0 && errors.All(e => e.ErrorCode == ErrorCode.UnknownTopicOrPartition)) {
                return -1L;
            }
            if (errors.Count > 1) {
                throw new ProtocolException(errors.First().ErrorCode);
            }
            return response.TopicPartitions[0].PartitionOffsets[0].Offsets[0];
        }

        public Int64[] Offset(String topic, Int32[] partitions, OffsetOption option) {
            if (partitions == null || partitions.Length == 0) {
                throw new ArgumentOutOfRangeException("partitions");
            }
            var offsets = new Int64[partitions.Length];
            for (int i = 0; i < partitions.Length; i++) {
                offsets[i] = Offset(topic, partitions[i], option);
            }
            return offsets;
        }

        public Int64 OffsetFetch(String topic, Int32 partition, String groupId) {
            EnsureLegalTopicSpelling(topic);
            var broker = _topicBrokerDispatcher.Select(topic, partition);
            var request = new OffsetFetchRequest(topic, new[] { partition }, groupId);
            var response = (OffsetFetchResponse)SubmitRequest(broker.ToUri(), request);
            var errors = response.TopicPartitions
                .SelectMany(r => r.Details)
                .Where(x => x.ErrorCode != ErrorCode.NoError)
                .ToList();
            if (errors.Count > 0 && errors.All(e => e.ErrorCode == ErrorCode.UnknownTopicOrPartition)) {
                return -1L;
            }
            if (errors.Count > 1) {
                throw new ProtocolException(errors.First().ErrorCode);
            }
            return response.TopicPartitions[0].Details[0].Offset;
        }

        public Int64[] OffsetFetch(String topic, Int32[] partitions, String groupId) {
            if (partitions == null || partitions.Length == 0) {
                throw new ArgumentOutOfRangeException("partitions");
            }
            var offsets = new Int64[partitions.Length];
            for (int i = 0; i < partitions.Length; i++) {
                offsets[i] = OffsetFetch(topic, partitions[i], groupId);
            }
            return offsets;
        }

        public ErrorCode OffsetCommit(String topic, Int32 partition, String groupId, Int64 offset) {
            EnsureLegalTopicSpelling(topic);
            var broker = _topicBrokerDispatcher.Select(topic, partition);
            var request = OffsetCommitRequest.CreateV0(topic, partition, groupId, offset);
            var response = (OffsetCommitResponse)SubmitRequest(broker.ToUri(), request);
            response.TryThrowFirstErrorOccured();
            return response.TopicPartitions[0].Details[0].ErrorCode;
        }

        public void OffsetCommit(String topic, Int32[] partitions, String groupId, Int64[] offsets) {
            if (partitions == null || partitions.Length == 0) {
                throw new ArgumentOutOfRangeException("partitions");
            }
            if (offsets == null || offsets.Length == 0) {
                throw new ArgumentOutOfRangeException("offsets");
            }
            if (partitions.Length != offsets.Length) {
                throw new ArgumentException("Array length of match");
            }
            for (int i = 0; i < partitions.Length; i++) {
                OffsetCommit(topic, partitions[i], groupId, offsets[i]);
            }
        }
    }
}
