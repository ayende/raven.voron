using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;

namespace Vizelia.Storage
{
    public class Perf
    {
        static void Main(string[] args)
        {
            //File.Delete("test.perf");
            using (var storage = new MeterDataStorage("test.perf"))
            {
                //var now = DateTime.Now;
                var sp = Stopwatch.StartNew();
                //for (int x = 0; x < 10; x++)
                //{
                //    for (int i = 0; i < 1000; i++)
                //    {
                //        var list = new List<MD>();
                //        for (int j = 0; j < 1000; j++)
                //        {
                //            now = now.AddMilliseconds(1);
                //            list.Add(new MD
                //            {
                //                KeyMeter = i,
                //                AcquisitionDateTime = now,
                //                Validity = (byte)j,
                //                Value = i * j
                //            });
                //        }
                //        storage.AddRange(list);
                //    }
                //}

                var rand = new Random();
                for (int i = 0; i < 100; i++)
                {
                    storage.Get(new MeterDataQuery {KeyMeter = rand.Next(1, 1000)}).ToList();
                }
                Console.WriteLine("{0:#,#;;0}", sp.ElapsedMilliseconds);
            }
        } 
    }
}