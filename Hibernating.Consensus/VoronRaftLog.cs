using System;
using System.IO;
using System.Text;
using Hibernating.Consensus.Messages;
using Newtonsoft.Json;
using Voron;
using Voron.Impl;
using Voron.Trees;

namespace Hibernating.Consensus
{
    public class VoronRaftLog : IRaftLog
    {
        private readonly RaftEngine _raftEngine;
        private const string CurrentVersion = "1.0";

        private readonly StorageEnvironment _storageEnvironment;

        private Tree _logsTree;
        private Tree _metadataTree;
        private Tree _peersTree;

        public Transaction Transaction { get; set; }
        public EntryId Commit { get; set; }

        public Tree PeersTree
        {
            get
            {
                if (_peersTree == null)
                    _peersTree = Transaction.ReadTree("peers");
                return _peersTree;
            }
        }

        public Tree LogsTree
        {
            get
            {
                if (_logsTree == null)
                    _logsTree = Transaction.ReadTree("logs");
                return _logsTree;
            }
        }

        public Tree MetadataTree
        {
            get
            {
                if (_metadataTree == null)
                    _metadataTree = Transaction.ReadTree("$metadata");
                return _metadataTree;
            }
        }


        public VoronRaftLog(StorageEnvironmentOptions options, RaftEngine raftEngine)
        {
            _raftEngine = raftEngine;
            _storageEnvironment = new StorageEnvironment(options);
            try
            {
                InitializeDatabase();
            }
            catch (Exception)
            {
                Dispose();
                throw;
            }
        }

        private void InitializeDatabase()
        {
            using (var tx = _storageEnvironment.NewTransaction(TransactionFlags.ReadWrite))
            {
                _storageEnvironment.CreateTree(tx, "logs");
                var peers = _storageEnvironment.CreateTree(tx, "peers");

                using (var it = peers.Iterate(tx)) // read all the known peers
                {
                    var serializer = new JsonSerializer();
                    if (it.Seek(Slice.BeforeAllKeys))
                    {
                        do
                        {
                            var reader = it.CreateReaderForCurrent();
                            using (var stream = reader.AsStream())
                            {
                                var raftPeer = serializer.Deserialize<RaftPeer>(new JsonTextReader(new StreamReader(stream)));
                                _raftEngine.AddPeerInternal(raftPeer);
                            }
                        } while (it.MoveNext());
                    }
                }

                var metadata = _storageEnvironment.CreateTree(tx, "$metadata");

                var versionReadResult = metadata.Read(tx, "version");
                if (versionReadResult == null) // new db
                {
                    metadata.Add(tx, "version", new MemoryStream(Encoding.UTF8.GetBytes(CurrentVersion)));
                    metadata.Add(tx, "db-id", new MemoryStream(Guid.NewGuid().ToByteArray()));
                }
                else
                {
                    var dbVersion = versionReadResult.Reader.ToStringValue();
                    if (dbVersion != CurrentVersion)
                        throw new InvalidOperationException("Cannot open db because its version is " + dbVersion +
                                                            " but the library expects version " + CurrentVersion);
                }

                tx.Commit();
            }
        }


        public void Dispose()
        {
            if(_storageEnvironment != null)
                _storageEnvironment.Dispose();
        }

        public void CommitScope()
        {
            Transaction.Commit();
        }

        public void ExitScope()
        {
             Transaction.Dispose();
            _logsTree = null;
            _metadataTree = null;
            _peersTree = null;
            Transaction = null;
        }

        public void EnterScope()
        {
            Transaction = _storageEnvironment.NewTransaction(TransactionFlags.ReadWrite);
        }

        public void AddPeer(RaftPeer peer)
        {
            var serializer = new JsonSerializer();
            var memoryStream = new MemoryStream();
            serializer.Serialize(new StreamWriter(memoryStream), peer);
            memoryStream.Position = 0;
            PeersTree.Add(Transaction, peer.Name, memoryStream);
        }

        public void AddEntry(EntryId entry, Stream stream)
        {
            var slice = entry.ToSlice();
            LogsTree.Add(Transaction, slice, stream);
        }

        public Stream ReadEntry(EntryId entry)
        {
            // we want to read it from memory, we cannot read twice from 
            // the user's stream
            var read = LogsTree.Read(Transaction, entry.ToSlice());
            if (read == null)
                return null;
            return read.Reader.AsStream();
        }


        public EntryId? GetLastLogEntry()
        {
            using (var it = LogsTree.Iterate(Transaction))
            {
                if (it.Seek(Slice.AfterAllKeys)) // has entries in the logs
                {
                    return new EntryId(it.CurrentKey);
                }
                return null;
            }
        }

        public void ApplyCommit(EntryId entryToCommit, Action<EntryId, Stream> action)
        {
            // It is possible to get a leader that will try to commit already committed
            // entries, that can happen because we only select leaders based on who has the most
            // up to date entries, not the most up to date committed entries, we can ignore it in 
            // this case
            if (entryToCommit <= Commit)
                return;

            // Raft article §5.3


            using (var it = LogsTree.Iterate(Transaction))
            {
                var commitPoint = Commit.ToSlice();
                if (it.Seek(commitPoint) == false)
                {
                    throw new InvalidOperationException("Could not find committed entry " + Commit + " in the log");
                }
                if (it.CurrentKey.Equals(commitPoint) == false)
                {
                    if (Commit.Term != 0 && Commit.Index != 0) // not relevant for the very first entry
                        throw new InvalidOperationException("Could not find committed entry " + Commit + " in the log");
                }
                else if (it.MoveNext() == false) // skip the already committed entry
                {
                    return; // nothing to commit after this
                }

                do
                {
                    var entry = new EntryId(it.CurrentKey);

                    if (entry > entryToCommit)
                        break;

                    var reader = it.CreateReaderForCurrent();
                    using (var stream = reader.AsStream())
                    {
                        action(entry, stream);
                    }
                } while (it.MoveNext());
            }
            MetadataTree.Add(Transaction, "commit-index", entryToCommit.ToStream());
            Commit = entryToCommit;
        }

        public bool TruncateLogAfter(EntryId entry)
        {
            if (entry < Commit)
                throw new InvalidOperationException("Index is already commited, cannot truncate. Commit: " + Commit +
                                                    ", requested truncation point: " + entry);
            var key = entry.ToSlice();


            using (var it = LogsTree.Iterate(Transaction))
            {
                if (it.Seek(key) == false)
                    return false; // we couldn't find the term/index pair

                if (it.CurrentKey.Equals(key) == false)
                {
                    var currentEntry = new EntryId(it.CurrentKey);
                    throw new InvalidOperationException("Could not find given entry (" + entry +
                                                        "), but found term/index _after_ them (" + currentEntry + ")");
                }

                if (it.MoveNext() == false)
                    return true; // nothing after this, nothing to truncate

                // there is stuff after this, which haven't been commited, we need to remove it
                // so we can match the leader's view of the world
                while (it.DeleteCurrentAndMoveNext())
                {
                }
                return true;
            }
        }
    }
}