using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Chuye.Kafka {
    public interface ISerialize {
        void WriteTo(Stream stream);
        Task WriteToAsync(Stream stream);
    }

    public interface IDeserialize {
        void ReadFrom(Stream stream);
        Task ReadFromAsync(Stream stream);
    }

    public abstract class Request : ISerialize {
        public void WriteTo(Stream stream) {
            throw new NotImplementedException();
        }

        public Task WriteToAsync(Stream stream) {
            throw new NotImplementedException();
        }
    }
    public abstract class Response : IDeserialize {
        public void ReadFrom(Stream stream) {
            throw new NotImplementedException();
        }

        public Task ReadFromAsync(Stream stream) {
            throw new NotImplementedException();
        }
    }
}
