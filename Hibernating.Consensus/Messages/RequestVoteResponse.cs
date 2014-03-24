namespace Hibernating.Consensus.Messages
{
	public class RequestVoteResponse
	{
		public long Term { get; set; }
		public bool VoteGranted { get; set; }
	}
}