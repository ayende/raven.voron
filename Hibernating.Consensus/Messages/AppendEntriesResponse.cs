namespace Hibernating.Consensus.Messages
{
	public class AppendEntriesResponse
	{
		public long Term { get; set; }
		public EntryId Entry { get; set; }
		public EntryId Commit { get; set; }
		public bool Success { get; set; }
		public string From { get; set; }
	}
}