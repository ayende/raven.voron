using System;
using System.IO;
using Hibernating.Consensus.Messages;

namespace Hibernating.Consensus
{
	public class CommitEntryEventArges : EventArgs
	{
		public Stream Stream;
		public EntryId Entry;
	}
}