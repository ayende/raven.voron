using System;
using Hibernating.Consensus.Messages;

namespace Hibernating.Consensus
{
	public class RaftPeer
	{
		public string Name { get; set; } 
		public long LastKnownIndex { get; set; }
		public DateTime LastSeen { get; set; }
	}
}