using System.Collections.Concurrent;
using Hibernating.Consensus.Messages;

namespace Hibernating.Consensus
{
	public interface ITransport
	{
		void Send(string name, RequestVoteRequest req);
		void Send(string name, AppendEntriesRequest req);

		void Send(string name, AppendEntriesResponse resp);
		void Send(string name, ErrorReportEvent resp);
		void Send(string name, RequestVoteResponse resp);

		void SetSink(BlockingCollection<Envelope> sink);
	}
}