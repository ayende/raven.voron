using System;
using System.Collections.Generic;
using System.IO;
using MiscUtil.Conversion;
using Voron;
using Voron.Impl;

namespace Vizelia.Storage
{
    public class MeterDataStorage : IDisposable
    {
        private readonly StorageEnvironment _environment;

        public MeterDataStorage(string file)
        {
            _environment = new StorageEnvironment(new MemoryMapPager(file));
        }

        public void AddRange(params MD[] range)
        {
            AddRange((IEnumerable<MD>)range);
        }
        /// <summary>
        /// Add a range of meter data, the primary key here is the KeyMeter + AcquisitionDateTime
        /// </summary>
        public void AddRange(IEnumerable<MD> range)
        {
            var bytes = new byte[12];
            var slice = new Slice(bytes);
            using (var ms = new MemoryStream())
            using (var bw = new BinaryWriter(ms))
            using (var tx = _environment.NewTransaction(TransactionFlags.ReadWrite))
            {
                foreach (var md in range)
                {
                    if (md.KeyMeter == 0) throw new ArgumentException("KeyMeter cannot be zero");

                    EndianBitConverter.Big.CopyBytes(md.KeyMeter, bytes, 0);
                    EndianBitConverter.Big.CopyBytes(md.AcquisitionDateTime.Ticks, bytes, 4);

                    ms.Position = 0;

                    bw.Write(md.Value);
                    bw.Write(md.Validity);

                    ms.Position = 0;

                    _environment.Root.Add(tx, slice, ms);

                }

                tx.Commit();
            }
        }

        public IEnumerable<MD> Get(MeterDataQuery query)
        {
            if (query == null) throw new ArgumentNullException("query");
            if (query.KeyMeter == 0) throw new ArgumentException("KeyMeter cannot be zero");

            using (var tx = _environment.NewTransaction(TransactionFlags.Read))
            {
                var startBytes = new byte[12];
                var start = new Slice(startBytes);

                var endBytes = new byte[12];
                var end = new Slice(endBytes);

                EndianBitConverter.Big.CopyBytes(query.KeyMeter, startBytes, 0);
                EndianBitConverter.Big.CopyBytes(query.KeyMeter, endBytes, 0);

                if (query.Start != null)
                {
                    var dateTime = query.Start.Value;
                    var startDate = dateTime.Ticks;
                    if (query.StartExclusive)
                        startDate++;

                    EndianBitConverter.Big.CopyBytes(startDate, startBytes, 4);
                }

                if (query.End != null)
                {
                    var endDate = query.End.Value.Ticks;
                    if (query.EndExclusive == false)
                        endDate++;

                    EndianBitConverter.Big.CopyBytes(endDate, endBytes, 4);
                }
                else
                {
                    EndianBitConverter.Big.CopyBytes(long.MaxValue, endBytes, 4);
                }

                var bytes = new byte[12];
                using (var it = _environment.Root.Iterate(tx))
                {
                    it.MaxKey = end;

                    if (it.Seek(start) == false)
                        yield break;
                    do
                    {
                        var currentKey = it.CurrentKey;
                        currentKey.CopyTo(bytes);
                        var md = new MD
                            {
                                KeyMeter = EndianBitConverter.Big.ToInt32(bytes, 0),
                                AcquisitionDateTime = new DateTime(EndianBitConverter.Big.ToInt64(bytes, 4))
                            };
                        using (var br = new BinaryReader(it.CreateStreamForCurrent()))
                        {
                            md.Value = br.ReadDouble();
                            md.Validity = br.ReadByte();
                        }
                        yield return md;
                    } while (it.MoveNext());
                }
            }
        }

        public void Dispose()
        {
            _environment.Dispose();
        }
    }

    public class MeterDataQuery
    {
        public int KeyMeter;
        public DateTime? Start;
        public DateTime? End;
        public bool StartExclusive;
        public bool EndExclusive;
    }
}