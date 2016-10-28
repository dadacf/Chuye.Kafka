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
        private readonly MessageBody _messageBody;

        public MessageSetDetail[] Items { get; set; }

        public MessageSet() {
        }

        public MessageSet(MessageBody messageBody) {
            _messageBody = messageBody;
        }

        public void FetchFrom(KafkaReader reader) {
            if (_messageBody.MessageSetSize == 0 || _messageBody.HighwaterMarkOffset == 0) {
                Items = new MessageSetDetail[0];
                return;
            }
            var previousPosition = reader.PositionProceeded;
            var items = new List<MessageSetDetail>(32);
            while (reader.PositionProceeded - previousPosition < _messageBody.MessageSetSize) {
                var item = new MessageSetDetail();
                item.FetchFrom(reader);
                items.Add(item);
            }
            Items = items.ToArray();
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
                    using (var reader = new KafkaReader(stream)) {
                        throw new NotImplementedException();
                        var set = new MessageSet();
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

        public virtual void SaveTo(KafkaWriter writer) {
            //N.B., MessageSets are not preceded by an int32 like other array elements in the protocol.
            //writer.Write(Items.Length); //Error
            //writer.Write(Items); //Error
            foreach (var item in Items) {
                item.SaveTo(writer);
            }
        }

    }

    public class GZipMessageSet : MessageSet {
        public override void SaveTo(KafkaWriter writer) {
            using (var stream = new MemoryStream(4096)) {
                var writer2 = new KafkaWriter(stream);
                foreach (var item in Items) {
                    item.SaveTo(writer2);
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
                base.SaveTo(writer);
            }
        }
    }

    public class MessageSetDetail : IKafkaReadable, IKafkaWriteable {
        public Int64 Offset { get; set; }
        public Int32 MessageSize { get; private set; }
        public MessageSetItem Message { get; set; }

        public void FetchFrom(KafkaReader reader) {
            Offset = reader.ReadInt64();        //move 8
            MessageSize = reader.ReadInt32();   //move 4
            Message = new MessageSetItem();
            Message.FetchFrom(reader);
        }

        public void SaveTo(KafkaWriter writer) {
            writer.Write(Offset);
            //writer.Write(MessageSize);
            var lengthWriter = new KafkaLengthWriter(writer);
            lengthWriter.MarkAsStart();
            Message.SaveTo(writer);
            MessageSize = lengthWriter.Caculate();
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

        public void FetchFrom(KafkaReader reader) {
            Crc = reader.ReadInt32();                       //move 4
            MagicByte = reader.ReadByte();                  //move 1
            Attributes = (MessageCodec)reader.ReadByte();   //move 1
            Key = reader.ReadBytes();                       //move 4 + len(bytes) if not null
            Value = reader.ReadBytes();                     //move 4 + len(bytes) if not null
        }

        public void SaveTo(KafkaWriter writer) {
            var crcWriter = new KafkaCrc32Writer(writer);
            crcWriter.MarkAsStart();

            writer.Write(MagicByte);
            writer.Write((Byte)Attributes);
            writer.Write(Key);
            writer.Write(Value);

            Crc = crcWriter.Caculate();
        }
    }
}
