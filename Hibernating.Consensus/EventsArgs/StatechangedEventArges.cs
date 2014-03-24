using System;

namespace Hibernating.Consensus
{
	public class StateChangedEventArges : EventArgs
	{
		public RaftEngineState State;
		public RaftEngineState PreviousState;
	}
}