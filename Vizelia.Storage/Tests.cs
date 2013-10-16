using System;
using System.Collections.Generic;
using System.IO;
using Vizelia.Storage;
using Xunit;
using System.Linq;

namespace Vizelia
{
    public class Tests : IDisposable
    {
        private readonly MeterDataStorage meterDataStorage;

        public Tests()
        {
            meterDataStorage = new MeterDataStorage("md.storage.test");
            meterDataStorage.AddRange(
                new MD { AcquisitionDateTime = DateTime.Today.AddDays(-1), KeyMeter = 2 },
                new MD { AcquisitionDateTime = DateTime.Today.AddDays(0), KeyMeter = 2 },
                new MD { AcquisitionDateTime = DateTime.Today.AddDays(1), KeyMeter = 2 }
                );
        }

        [Fact]
        public void FilterStart()
        {
            var mds = meterDataStorage.Get(new MeterDataQuery
                {
                    KeyMeter = 2,
                    Start = DateTime.Today,
                    StartExclusive = true
                }).ToList();

            Assert.Equal(1, mds.Count);

            mds = meterDataStorage.Get(new MeterDataQuery
            {
                KeyMeter = 2,
                Start = DateTime.Today,
                StartExclusive = false
            }).ToList();

            Assert.Equal(2, mds.Count);
        }

        [Fact]
        public void FilterEnd()
        {
            var mds = meterDataStorage.Get(new MeterDataQuery
            {
                KeyMeter = 2,
                End = DateTime.Today,
                EndExclusive = true
            }).ToList();

            Assert.Equal(1, mds.Count);

            mds = meterDataStorage.Get(new MeterDataQuery
            {
                KeyMeter = 2,
                End = DateTime.Today,
                EndExclusive = false
            }).ToList();

            Assert.Equal(2, mds.Count);
        }


        [Fact]
        public void ReadAndWrite()
        {

            var mds = meterDataStorage.Get(new MeterDataQuery{KeyMeter = 2}).ToList();

            Assert.Equal(3, mds.Count);
        }

        public void Dispose()
        {
            meterDataStorage.Dispose();
            File.Delete("md.storage.test");
        }
    }
}
