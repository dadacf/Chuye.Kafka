using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Chuye.Kafka.Serialization;

namespace Chuye.Kafka.Protocol {
    //MessageSet => [Offset MessageSize Message]
    //  Offset => int64
    //  MessageSize => int32    
    public class MessageSet : IKafkaReadable, IKafkaWriteable {
        private readonly Int32 _messageSetSize;

        public MessageSetDetail[] Items { get; set; }

        public MessageSet() {
        }

        public MessageSet(Int32 messageSetSize) {
            _messageSetSize = messageSetSize;
        }

        public void FetchFrom(KafkaStreamReader reader) {
            var previousPosition = reader.BaseStream.Position;
            var sets = new List<MessageSetDetail>();
            while (reader.BaseStream.Position - previousPosition < _messageSetSize) {
                //var expect = 14;
                var set = new MessageSetDetail();
                var messageStartPosition = reader.BaseStream.Position + 16;
                try {
                    set.FetchFrom(reader);
                    if (reader.BaseStream.Position - previousPosition > _messageSetSize) {
                        continue;
                    }
                    if (reader.BaseStream.Position - previousPosition == _messageSetSize) {
                        var messageLength = (Int32)(reader.BaseStream.Position - messageStartPosition);
                        if (messageLength != set.MessageSize - 4) {
                            continue;
                        }
                        var messageBuffer = new Byte[messageLength];
                        reader.BaseStream.Seek(messageStartPosition, SeekOrigin.Begin);
                        reader.BaseStream.Read(messageBuffer, 0, messageLength);
                        var crc32 = CRC32.ComputeHash(messageBuffer);
                        if (crc32 != set.Message.Crc) {
                            continue;
                        }
                    }
                    sets.Add(set);
                }
                catch (IndexOutOfRangeException) {
                    break;
                }
            }
            Items = Decompress(sets).ToArray();
        }

        private IEnumerable<MessageSetDetail> Decompress(IEnumerable<MessageSetDetail> sets) {
            foreach (var item in sets) {
                if (item.Message.Attributes == MessageCodec.None) {
                    yield return item;
                }
                else if (item.Message.Attributes == MessageCodec.Snappy) {
                    yield return item;
                }
                else if (item.Message.Attributes == MessageCodec.Gzip) {
                    var buffer = GZip.Decompress(item.Message.Value);
                    using (var stream = new MemoryStream(buffer))
                    using (var reader = new KafkaStreamReader(stream)) {
                        var set = new MessageSet((Int32)stream.Length);
                        set.FetchFrom(reader);
                        foreach (var item2 in set.Items) {
                            yield return item2;
                        }
                    }
                }
                else {
                    throw new NotImplementedException();
                }
            }
        }

        public virtual void WriteTo(KafkaStreamWriter writer) {
            //N.B., MessageSets are not preceded by an int32 like other array elements in the protocol.
            //writer.Write(Items.Length); //Error
            //writer.Write(Items); //Error
            foreach (var item in Items) {
                item.WriteTo(writer);
            }
        }

    }

    public class GZipMessageSet : MessageSet {
        public override void WriteTo(KafkaStreamWriter writer) {
            using (var stream = new MemoryStream(4096)) {
                var writer2 = new KafkaStreamWriter(stream);
                foreach (var item in Items) {
                    item.WriteTo(writer2);
                }
                var messageBuffer = stream.ToArray();
                var compressedMessageBuffer = GZip.Compress(messageBuffer, 0, messageBuffer.Length);

                Items = new[] {
                    new MessageSetDetail {
                        Message = new MessageSetItem {
                            Attributes = MessageCodec.Gzip,
                            Value      = compressedMessageBuffer,
                        }
                    }
                };
                writer2.Dispose();
                base.WriteTo(writer);
            }
        }
    }

    public class MessageSetDetail : IKafkaReadable, IKafkaWriteable {
        public Int64 Offset { get; set; }
        public Int32 MessageSize { get; private set; }
        public MessageSetItem Message { get; set; }

        public void FetchFrom(KafkaStreamReader reader) {
            Offset = reader.ReadInt64();
            MessageSize = reader.ReadInt32();
            Message = new MessageSetItem();
            Message.FetchFrom(reader);
        }

        public void WriteTo(KafkaStreamWriter writer) {
            writer.Write(Offset);
            //writer.Write(MessageSize);
            var lengthWriter = new KafkaLengthWriter(writer);
            lengthWriter.BeginWrite();
            Message.WriteTo(writer);
            MessageSize = lengthWriter.EndWrite();
        }
    }

    //Message => Crc MagicByte Attributes Key Value
    //  Crc => int32
    //  MagicByte => int8
    //  Attributes => int8
    //  Key => bytes
    //  Value => bytes
    public class MessageSetItem : IKafkaReadable, IKafkaWriteable {
        public Int32 Crc { get; private set; }
        public Byte MagicByte { get; set; }
        public MessageCodec Attributes { get; set; }
        public Byte[] Key { get; set; }
        public Byte[] Value { get; set; }

        public void FetchFrom(KafkaStreamReader reader) {
            Crc = reader.ReadInt32();
            MagicByte = reader.ReadByte();
            Attributes = (MessageCodec)reader.ReadByte();
            Key = reader.ReadBytes();
            Value = reader.ReadBytes();
        }

        public void WriteTo(KafkaStreamWriter writer) {
            var crcWriter = new KafkaBinaryCRCWriter(writer);
            crcWriter.BeginWrite();

            writer.Write(MagicByte);
            writer.Write((Byte)Attributes);
            writer.Write(Key);
            writer.Write(Value);

            Crc = crcWriter.EndWrite();
        }
    }
}
