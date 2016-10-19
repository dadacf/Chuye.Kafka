using System;
using System.Collections.Generic;
using System.Diagnostics;
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
        private readonly KnownBrokerDispatcher _knownBrokerDispatcher;
        private readonly TopicBrokerDispatcher _topicBrokerDispatcher;
        private readonly ConnectionFactory _connectionFactory;

        public event EventHandler<RequestSendingEventArgs> RequestSending;
        public event EventHandler<ResponseReceivedEventArg> ResponseReceived;
        
        internal KnownBrokerDispatcher ExistingBrokerDispatcher {
            get { return _knownBrokerDispatcher; }
        }

        internal TopicBrokerDispatcher TopicBrokerDispatcher {
            get { return _topicBrokerDispatcher; }
        }

        public Client(Option option) { 
            _connectionFactory = option.GetSharedConnections();
            _knownBrokerDispatcher = new KnownBrokerDispatcher(option.BrokerUris.ToArray());
            _topicBrokerDispatcher = new TopicBrokerDispatcher(this);
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

        internal Response SubmitRequest(Broker broker, Request req) {
            return SubmitRequest(broker.ToUri(), req);
        }

        internal Response SubmitRequest(Uri uri, Request req) {
            var reqEvent = new RequestSendingEventArgs(uri, req);
            OnRequestSending(reqEvent);
            //Trace.TraceInformation("{0:HH:mm:ss.fff} [{1:d2}] Sending {2} to {3}",
            //    DateTime.Now, Thread.CurrentThread.ManagedThreadId, reqEvent.Request.ApiKey, reqEvent.Uri.AbsoluteUri);
            using (var connection = _connectionFactory.Connect(reqEvent.Uri)) {
                var resp = connection.Submit(reqEvent.Request);
                //Trace.TraceInformation("{0:HH:mm:ss.fff} [{1:d2}] Received {2} from {3}",
                //    DateTime.Now, Thread.CurrentThread.ManagedThreadId, resp.GetType().Name, reqEvent.Uri.AbsoluteUri);
                var respEvent = new ResponseReceivedEventArg(resp);
                OnResponseReceived(respEvent);
                return respEvent.Response;
            }
        }

        internal Task<Response> SubmitRequestAsync(Broker broker, Request req) {
            return SubmitRequestAsync(broker, req);
        }


        internal async Task<Response> SubmitRequestAsync(Uri uri, Request req) {
            var reqEvent = new RequestSendingEventArgs(uri, req);
            OnRequestSending(reqEvent);
            //Trace.TraceInformation("{0:HH:mm:ss.fff} [{1:d2}] Sending {2} to {3}",
            //    DateTime.Now, Thread.CurrentThread.ManagedThreadId, reqEvent.Request.ApiKey, reqEvent.Uri.AbsoluteUri);
            using (var connection = _connectionFactory.Connect(reqEvent.Uri)) {
                var resp = await connection.SubmitAsync(reqEvent.Request);
                //Trace.TraceInformation("{0:HH:mm:ss.fff} [{1:d2}] Received {2} from {3}",
                //    DateTime.Now, Thread.CurrentThread.ManagedThreadId, resp.GetType().Name, reqEvent.Uri.AbsoluteUri);
                var respEvent = new ResponseReceivedEventArg(resp);
                OnResponseReceived(respEvent);
                return respEvent.Response;
            }
        }

        protected virtual void OnRequestSending(RequestSendingEventArgs @event) {
            RequestSending?.Invoke(this, @event);
        }

        protected virtual void OnResponseReceived(ResponseReceivedEventArg @event) {
            ResponseReceived?.Invoke(this, @event);
        }

        public MetadataResponse Metadata(params String[] topics) {
            if (topics == null) {
                topics = new String[0];
            }
            foreach (var topic in topics) {
                EnsureLegalTopicSpelling(topic);
            }

            var brokerUri = _knownBrokerDispatcher.FreeSelect();
            var request = new MetadataRequest(topics);
            var response = (MetadataResponse)SubmitRequest(brokerUri, request);
            response.TopicMetadatas = response.TopicMetadatas
                .Where(x => x.TopicName != __consumer_offsets).ToArray();
            return response;
        }

        public Int64 Produce(String topic, Int32 partition, IList<Message> messages) {
            EnsureLegalTopicSpelling(topic);
            var broker = _topicBrokerDispatcher.SelectBroker(topic, partition);
            var request = new ProduceRequest(topic, partition, messages);
            var response = (ProduceResponse)SubmitRequest(broker, request);
            response.TryThrowFirstErrorOccured();
            return response.TopicPartitions[0].Details[0].Offset;
        }

        public async Task<Int64> ProduceAsync(String topic, Int32 partition, IList<Message> messages) {
            EnsureLegalTopicSpelling(topic);
            var broker = _topicBrokerDispatcher.SelectBroker(topic, partition);
            var request = new ProduceRequest(topic, partition, messages);
            var response = (ProduceResponse)(await SubmitRequestAsync(broker.ToUri(), request));
            response.TryThrowFirstErrorOccured();
            return response.TopicPartitions[0].Details[0].Offset;
        }

        public Int64 Produce(String topic, Int32 partition, params String[] messages) {
            return Produce(topic, partition, messages.Select(x => (Message)x).ToArray());
        }

        public IEnumerable<OffsetMessage> Fetch(String topic, Int32 partition, Int64 fetchOffset) {
            EnsureLegalTopicSpelling(topic);
            var broker = _topicBrokerDispatcher.SelectBroker(topic, partition);
            var request = new FetchRequest(topic, partition, fetchOffset);
            var response = (FetchResponse)SubmitRequest(broker, request);
            response.TryThrowFirstErrorOccured();
            return response.TopicPartitions.SelectMany(x => x.MessageBodys)
                .SelectMany(x => x.MessageSet.Items)
                .Where(x => x.Offset >= fetchOffset)
                .Select(msg => new OffsetMessage(msg.Offset, msg.Message.Key, msg.Message.Value));
        }

        public Int64 Offset(String topic, Int32 partition, OffsetOption option) {
            EnsureLegalTopicSpelling(topic);
            var broker = _topicBrokerDispatcher.SelectBroker(topic, partition);
            var request = new OffsetRequest(topic, new[] { partition }, option);
            var response = (OffsetResponse)SubmitRequest(broker, request);
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
            var broker = _topicBrokerDispatcher.SelectBroker(topic, partition);
            var request = new OffsetFetchRequest(topic, new[] { partition }, groupId);
            var response = (OffsetFetchResponse)SubmitRequest(broker, request);
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
            var broker = _topicBrokerDispatcher.SelectBroker(topic, partition);
            var request = OffsetCommitRequest.CreateV0(topic, partition, groupId, offset);
            var response = (OffsetCommitResponse)SubmitRequest(broker, request);
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
