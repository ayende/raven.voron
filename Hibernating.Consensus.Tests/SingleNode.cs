using System.Collections.Generic;
using System.IO;
using Voron;
using Xunit;

namespace Hibernating.Consensus.Tests
{
    public class SingleNode
    {
	    [Fact]
	    public void CanConstructAndDispose()
	    {
		    using (var x = CreateSingleNodeEngine())
		    {
			    
		    }
	    }

		[Fact]
		public void CanCommitSingleEntryUsingSingleNode()
		{
			using (var x = CreateSingleNodeEngine())
			{
				var committed = false;
				x.CommitEntry += (sender, args) =>
				{
					committed = true;
					Assert.Equal(42, args.Stream.ReadByte());
				};


				x.AppendAsync(new MemoryStream(new[] {(byte) 42})).Wait();

				Assert.True(committed);
			}
		}

		[Fact]
		public void CanCommitMultipleEntriesUsingSingleNode()
		{
			using (var x = CreateSingleNodeEngine())
			{
				var list = new List<byte>();
				x.CommitEntry += (sender, args) => list.Add((byte)args.Stream.ReadByte());


				for (int i = 0; i < 19; i++)
				{
					x.AppendAsync(new MemoryStream(new[] { (byte)i })).Wait();
				}

				Assert.Equal(list.Count, 19);
				for (int i = 0; i < 19; i++)
				{
					Assert.Equal(i, list[i]);
				}
			}
		}


	    private static RaftEngine CreateSingleNodeEngine()
	    {
		    return new RaftEngine(new RaftEngineSetup
		    {
			    Name = "self",
				Transport = new InMemoryTransport("self"),
				ElectionTimeoutMilliseconds = 5000,
				Options = StorageEnvironmentOptions.CreateMemoryOnly()
		    });
	    }
    }
}
