using System;

namespace Hibernating.Consensus
{
	public class LeaderChangedEventArgs : EventArgs
	{
		public string Leader;
		public string PreviousLeader;
	}
}