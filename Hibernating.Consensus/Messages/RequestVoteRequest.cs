namespace Hibernating.Consensus.Messages
{
	public class RequestVoteRequest
	{
		public long Term { get; set; }
		public long LastLogIndex { get; set; }
		public long LastLogTerm { get; set; }
		public string Candidate { get; set; }
	}
}