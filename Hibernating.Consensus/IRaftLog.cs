using System;
using System.IO;
using Hibernating.Consensus.Messages;

namespace Hibernating.Consensus
{
    public interface IRaftLog : IDisposable
    {
        void CommitScope();
        void ExitScope();
        void EnterScope();
        void AddPeer(RaftPeer peer);
        void AddEntry(EntryId entry, Stream stream);
        Stream ReadEntry(EntryId entry);
        EntryId? GetLastLogEntry();
        void ApplyCommit(EntryId entryToCommit, Action<EntryId, Stream> action);
        bool TruncateLogAfter(EntryId entry);
        EntryId Commit { get; }
    }
}