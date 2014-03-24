using System.IO;
using Newtonsoft.Json;

namespace Hibernating.Consensus.Messages
{
	public class AppendEntriesRequest
	{
		public long Term { get; set; }
		public EntryId Commit { get; set; }
		public EntryId PrevLog { get; set; }
		public string Leader { get; set; }

		public AppendEntry[] Entries { get; set; }
		
	}

	public class AppendEntry
	{
		public EntryId Entry { get; set; }

        [JsonIgnore]
		public Stream Stream { get; set; }
	}
}