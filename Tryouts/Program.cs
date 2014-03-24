using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Hibernating.Consensus.Tests;

namespace Tryouts
{
    class Program
    {
        static void Main(string[] args)
        {
            for (int i = 0; i < 10; i++)
            {
                Console.Clear();
                Console.WriteLine(i);
                using (var x = new TripleNodes())
                    x.WillSelectLeader();
            }
        }
    }
}
