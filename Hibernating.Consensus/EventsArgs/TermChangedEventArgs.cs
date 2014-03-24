using System;

namespace Hibernating.Consensus
{
	public class TermChangedEventArgs : EventArgs
	{
		public long CurrentTerm;
		public long PreviousTerm;
	}
}