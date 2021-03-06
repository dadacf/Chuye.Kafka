﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Chuye.Kafka.Internal {
    class KnownBrokerDispatcher {
        private readonly IList<Uri> _existingbrokerUris;
        private Int32 _sequence;

        public KnownBrokerDispatcher(IList<Uri> existingbrokerUris) {
            _existingbrokerUris = existingbrokerUris;
            _sequence = 0;
        }

        public Uri FreeSelect() {
            var sequence = DateTime.UtcNow.Millisecond % _existingbrokerUris.Count;
            return _existingbrokerUris[sequence];
        }

        public Uri SequentialSelect() {
            return _existingbrokerUris[_sequence++ % _existingbrokerUris.Count];
        }
    }

}
