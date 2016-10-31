using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Chuye.Kafka.Protocol;

namespace Chuye.Kafka.Internal {
    class ConsumerOffsetRecorder {
        private readonly Client _client;
        private readonly String _groupId;
        private readonly String _topic;
        private readonly Int32[] _partitions;
        private readonly Int64[] _offsetSaved;
        private readonly Int64[] _offsetSubmited;
        private DateTime _lastSubmit;
        private Object _sync;

        public ConsumerOffsetRecorder(Consumer consumer, Int32[] partitions) {
            _client         = consumer.Client;
            _groupId        = consumer.GroupId;
            _topic          = consumer.Topic;
            _partitions     = partitions;
            _offsetSaved    = Enumerable.Repeat(-1L, partitions.Length).ToArray();
            _offsetSubmited = new Int64[partitions.Length];
            _lastSubmit     = DateTime.UtcNow;
            _sync           = new Object();
        }

        public void MoveForward(Int32 partition) {
            var index = Array.IndexOf(_partitions, partition);
            if (_offsetSubmited[index] < _offsetSaved[index]) {
                OffsetCommit(partition, _offsetSaved[index]);
                _offsetSubmited[index] = _offsetSaved[index];
            }
        }

        public void MoveForward(Int32 partition, Int64 offset) {
            MoveForward(partition, offset, false);
        }

        public void MoveForward(Int32 partition, Int64 offset, Boolean forceSubmit) {
            var index = Array.IndexOf(_partitions, partition);
            if (_offsetSaved[index] > offset + 1) {
                throw new ArgumentOutOfRangeException("offset");
            }
            if (_offsetSubmited[index] == offset + 1) {
                return;
            }

            lock (_sync) {
                _offsetSaved[index] = offset + 1;
                var shouldSubmit = forceSubmit
                    || (_offsetSubmited[index] + 9 <= _offsetSaved[index]
                    && _lastSubmit.Subtract(DateTime.UtcNow).TotalSeconds > 2d);
                if (shouldSubmit) {
                    OffsetCommit(partition, _offsetSaved[index]);
                    _offsetSubmited[index] = _offsetSaved[index];
                }
            }
        }

        private void OffsetCommit(Int32 partition, Int64 offset) {
            Trace.TraceInformation("{0:HH:mm:ss.fff} [{1:d2}] Offset commit group '{2}', topic '{3}'[{4}], offset {5}",
                DateTime.Now, Thread.CurrentThread.ManagedThreadId, _groupId, _topic, partition, offset);
            _client.OffsetCommit(_topic, partition, _groupId, offset);
        }

        public Int64 GetCurrentOffset(Int32 partition) {
            var index = Array.IndexOf(_partitions, partition);
            var offsetSaved = _offsetSaved[index];
            lock (_sync) {
                if (offsetSaved == -1L) {
                    offsetSaved = _client.OffsetFetch(_topic, partition, _groupId);
                }
                if (offsetSaved == -1L) {
                    offsetSaved = _client.Offset(_topic, partition, OffsetOption.Earliest);
                    _offsetSaved[index] = offsetSaved;
                    _offsetSubmited[index] = offsetSaved;
                }
            }
            return offsetSaved;
        }
    }
}

