using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Voron;
using Xunit;

namespace Hibernating.Consensus.Tests
{
	public class TripleNodes : IDisposable
	{
		readonly List<RaftEngine> _engines = new List<RaftEngine>();

		public TripleNodes()
		{
			var t1 = new InMemoryTransport("one");
			var t2 = new InMemoryTransport("two");
			var t3 = new InMemoryTransport("three");
			const int electionTimeoutMilliseconds = 50;
			_engines.Add(new RaftEngine(new RaftEngineSetup
			{
				ElectionTimeoutMilliseconds = electionTimeoutMilliseconds,
				Name = "one",
				Options = StorageEnvironmentOptions.CreateMemoryOnly(),
				Peers = {"two", "three"},
				Transport = t1
			}));
			_engines.Add(new RaftEngine(new RaftEngineSetup
			{
				ElectionTimeoutMilliseconds = electionTimeoutMilliseconds,
				Name = "two",
				Options = StorageEnvironmentOptions.CreateMemoryOnly(),
				Peers = { "one", "three" },
				Transport = t2
			}));
			_engines.Add(new RaftEngine(new RaftEngineSetup
			{
				ElectionTimeoutMilliseconds = electionTimeoutMilliseconds,
				Name = "three",
				Options = StorageEnvironmentOptions.CreateMemoryOnly(),
				Peers = { "two", "one" },
				Transport = t3
			}));

			t1.BidirectionalConnectionTo(t2);
			t2.BidirectionalConnectionTo(t3);
			t3.BidirectionalConnectionTo(t1);
		}

		[Fact]
		public void CanCreateAndDestory()
		{
			Assert.Equal(3, _engines.Count);	
		}

		[Fact]
		public void WillSelectLeader()
		{
			Assert.True(SpinWait.SpinUntil(() => _engines.Any(x => x.State == RaftEngineState.Leader), TimeSpan.FromMilliseconds(2500)));
			Assert.Equal(1, _engines.Count(x => x.State == RaftEngineState.Leader));
		}

		public void Dispose()
		{
			foreach (var engine in _engines)
			{
				engine.Dispose();
			}
		}
	}
}