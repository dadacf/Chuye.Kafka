using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Chuye.Kafka.Serialization {
    internal static class KafkaWriterExtension {
        public static KafkaWriter Write(this KafkaWriter writer, Int32[] value) {
            if (value == null) {
                writer.Write(-1);
                return writer;
            }

            writer.Write((Int32)value.Length);
            foreach (var item in value) {
                writer.Write(item);
            }
            return writer;
        }

        public static KafkaWriter Write(this KafkaWriter writer, Int64[] value) {
            if (value == null) {
                writer.Write(-1);
                return writer;
            }

            writer.Write((Int32)value.Length);
            foreach (var item in value) {
                writer.Write(item);
            }
            return writer;
        }

        public static KafkaWriter Write(this KafkaWriter writer, String[] value) {
            if (value == null) {
                writer.Write(-1);
                return writer;
            }

            writer.Write((Int32)value.Length);
            foreach (var item in value) {
                writer.Write(item);
            }
            return writer;
        }

        public static KafkaWriter Write<T>(this KafkaWriter writer, ICollection<T> value) where T : IKafkaWriteable {
            if (value == null) {
                writer.Write(-1);
                return writer;
            }

            writer.Write((Int32)value.Count);
            foreach (var item in value) {
                item.SaveTo(writer);
            }
            return writer;
        }
    }
}
