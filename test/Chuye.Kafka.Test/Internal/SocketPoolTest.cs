using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Chuye.Kafka.Internal;
using Chuye.Kafka.Protocol;
using Xunit;

namespace Chuye.Kafka.Test.Internal {
    public class SocketPoolTest {
        [Fact]
        public void Pool_object_should_stock_when_dispose() {
            var sockets = new SocketPool(new Uri("http://www.baidu.com"));
            var socket = sockets.AcquireItem();

            socket.Dispose();
            Assert.True(socket.Connected);
        }

        [Fact]
        public void Pool_object_should_dispose_when_pool_release() {
            var sockets = new SocketPool(new Uri("http://www.baidu.com"));
            var socket = sockets.AcquireItem();

            sockets.ReleaseAll();
            Assert.True(sockets.MarkAsReleased);

            socket.Dispose();
            Assert.False(socket.Connected);
        }
    }
}
