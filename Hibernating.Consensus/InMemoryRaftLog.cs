using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Hibernating.Consensus.Messages;

namespace Hibernating.Consensus
{
    public class InMemoryRaftLog : IRaftLog
    {
        private readonly SortedDictionary<EntryId, Stream> _logs = new SortedDictionary<EntryId, Stream>();
        private readonly List<RaftPeer> _peers = new List<RaftPeer>();

        private SortedDictionary<EntryId, Stream> _logsScope;
        private List<RaftPeer> _peersScope;


        public void Dispose()
        {
            
        }

        public void CommitScope()
        {
            foreach (var raftPeer in _peersScope)
            {
                _peers.Add(raftPeer);
            }
            foreach (var kvp in _logsScope)
            {
                if (kvp.Value != null)
                    _logs[kvp.Key] = kvp.Value;
                else
                    _logs.Remove(kvp.Key);
            }
        }

        public void ExitScope()
        {
            _peersScope = null;
            _logsScope = null;
        }

        public void EnterScope()
        {
            _peersScope = new List<RaftPeer>();
            _logsScope = new SortedDictionary<EntryId, Stream>();
        }

        public void AddPeer(RaftPeer peer)
        {
            _peersScope.Add(peer);  
        }

        public void AddEntry(EntryId entry, Stream stream)
        {
            _logsScope.Add(entry, stream);
        }

        public Stream ReadEntry(EntryId entry)
        {
            Stream s;
            if (_logsScope.TryGetValue(entry, out s))
                return s;
            _logs.TryGetValue(entry, out s);
            return s;
        }

        public EntryId? GetLastLogEntry()
        {
            if (_logsScope.Count > 0)
                return _logsScope.LastOrDefault().Key;
            if(_logs.Count > 0)
                return _logs.LastOrDefault().Key;
            return null;
        }

        public void ApplyCommit(EntryId entryToCommit, Action<EntryId, Stream> action)
        {
            if (entryToCommit <= Commit)
                return;


            foreach (var kvp in _logs.Concat(_logsScope))
            {
                if(kvp.Key <= Commit)
                    continue;
                if (kvp.Key > entryToCommit)
                    break;

                kvp.Value.Position = 0;
                action(kvp.Key, kvp.Value);
            }

            Commit = entryToCommit;
        }

        public bool TruncateLogAfter(EntryId entry)
        {
            if (entry < Commit)
                throw new InvalidOperationException("Index is already commited, cannot truncate. Commit: " + Commit +
                                                    ", requested truncation point: " + entry);

            var entryInLog = _logs.ContainsKey(entry) == false;
            if (entryInLog)
            {
                if (_logsScope.ContainsKey(entry) == false)
                    return false; // couldn't find it
            }
            foreach (var e in _logsScope.Keys.Where(x => x > entry).ToList())
            {
                _logsScope[e] = null;
            }

            if (!entryInLog) 
                return true;

            foreach (var e in _logs.Keys.Where(x => x > entry))
            {
                _logsScope[e] = null; // delete...
            }
            return true;

        }

        public EntryId Commit { get; private set; }
    }
}