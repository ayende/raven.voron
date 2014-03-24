using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Hibernating.Consensus.Messages;
using Newtonsoft.Json;
using Voron;
using Voron.Impl;
using Voron.Trees;

namespace Hibernating.Consensus
{
	public class RaftEngine : IDisposable
	{
		private static TextWriter Log = Console.Out;
        private Dictionary<Type, Action<RaftEngine, string, object>> _handlers = new Dictionary<Type, Action<RaftEngine, string, object>>();
		private readonly Random _rnd = new Random();
		private readonly int _electionTimeoutMilliseconds;
		private int? _hungElectionTimeoutMilliseconds;
		private readonly Dictionary<string, RaftPeer> _peers = new Dictionary<string, RaftPeer>();
		private string _currentLeader;
		private long _currentTerm;
		private volatile RaftEngineState _state;

		private readonly SortedDictionary<EntryId, TaskCompletionSource<object>> _pending = new SortedDictionary<EntryId, TaskCompletionSource<object>>();
		private readonly BlockingCollection<Envelope> _events = new BlockingCollection<Envelope>();
		private readonly CancellationTokenSource _cancellationTokenSource = new CancellationTokenSource();

		private readonly Task _eventLoopTask;
	

	    private readonly IRaftLog _log;

		public RaftEngine(RaftEngineSetup raftEngineSetup)
		{
			raftEngineSetup.Validate();

			_electionTimeoutMilliseconds = raftEngineSetup.ElectionTimeoutMilliseconds;
			Name = raftEngineSetup.Name;
			Transport = raftEngineSetup.Transport;
			Transport.SetSink(_events);

		    _log = new InMemoryRaftLog(); //new VoronRaftLog(raftEngineSetup.Options, this);

			foreach (var peer in raftEngineSetup.Peers)
			{
				_peers.Add(peer, new RaftPeer {  Name = peer });
				// explicitly not adding to storage yet here
			}

		    if (_peers.Count == 0)
		    {
		        State = RaftEngineState.Leader;
		    }

            _eventLoopTask = Task.Run((Action)EventLoop);
		}

		public void AddPeer(string name)
		{
			if (State != RaftEngineState.Leader)
				throw new InvalidOperationException("Not a leader");
			_events.Add(new Envelope
			{
				Message = new AddPeerCommand { Name = name },
				Source = Name
			});
		}

		public Task AppendAsync(Stream stream)
		{
			if (State != RaftEngineState.Leader)
				throw new InvalidOperationException("Not a leader, cannot append");

			var tcs = new TaskCompletionSource<object>();

			_events.Add(new Envelope
			{
				Message = new AppendCommand
				{
					CompletionSource = tcs,
					Stream = stream
				},
				Source = Name
			});

			return tcs.Task;
		}

		private void EventLoop()
		{
			var cancellationToken = _cancellationTokenSource.Token;
			while (cancellationToken.IsCancellationRequested == false)
			{
			    Envelope env;
			    var timeout = _hungElectionTimeoutMilliseconds ??
			                  (State == RaftEngineState.Leader ? _electionTimeoutMilliseconds/4 : _electionTimeoutMilliseconds);
			    var hasValue = _events.TryTake(out env, timeout, cancellationToken);
			    if (cancellationToken.IsCancellationRequested)
			        break;
			    _log.EnterScope();
                try
			    {
			        if (hasValue == false)
			        {
			            if (State != RaftEngineState.Leader)
			                AnnounceCandidacy();
			            else
			                Heartbeat();
			        }
			        else
			            DispatchEvent(env);

			        _log.CommitScope();
			    }
			    catch (Exception e)
			    {
			        //TODO - proper logging
			        Console.WriteLine("Error processing event " + env.Message + " from " + env.Source);
			        Console.WriteLine(e);

			        Transport.Send(env.Source, new ErrorReportEvent
			        {
			            Error = e.ToString(),
			            Message = env.Message.ToString()
			        });
			    }
			    finally
			    {
			        _log.ExitScope();
			    }
			}
		}

		private void Heartbeat()
		{
			foreach (var peer in _peers)
			{
				Transport.Send(peer.Key, new AppendEntriesRequest
				{
					Term = CurrentTerm,
					Commit = _log.Commit,
					Leader = Name,
					Entries = new AppendEntry[0],
                    PrevLog = _log.GetLastLogEntry() ?? new EntryId()
				});
			}
		}

		private void DispatchEvent(Envelope env)
		{
            Action<RaftEngine,string, object> handler;
		    var msgType = env.Message.GetType();
		    if (_handlers.TryGetValue(msgType, out handler))
		    {
		        handler(this, env.Source, env.Message);
		        return;
		    }
		    var method = GetType()
		        .GetMethod("Handle", BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic, null,
		            new[] {msgType}, null);

		    if (method == null)
		        throw new InvalidOperationException("Cannot find a handler method for " + msgType);

		    var self = Expression.Parameter(typeof (RaftEngine), "self");
		    var src = Expression.Parameter(typeof (string), "src");
		    var msg = Expression.Parameter(typeof (object), "msg");

		    Expression body = Expression.Call(self, method, new Expression[] {Expression.Convert(msg, msgType)});

		    if (method.ReturnType != typeof (void))
		    {
		        var transport = Expression.MakeMemberAccess(self, GetType().GetProperty("Transport"));
		        var sendMethod = typeof(ITransport).GetMethod("Send", new[]{typeof(string), method.ReturnType});
                if(sendMethod == null)
                    throw new InvalidOperationException("Cannot find a send method for " + method.ReturnType);
                body = Expression.Call(transport, sendMethod,  src, Expression.Convert(body, method.ReturnType));
		    }

		    handler = Expression.Lambda<Action<RaftEngine, string, object>>(body, self, src, msg).Compile();
		    _handlers[msgType] = handler;
		    handler(this, env.Source, env.Message);
		}

	    public void Handle(ErrorReportEvent error)
	    {
            Log.WriteLine(error.Message);
	        Log.WriteLine(error.Error);
	    }

		private void Handle(RemvoePeerCommand remvoePeerCommand)
		{
			throw new NotImplementedException();
		}

		private void Handle(AddPeerCommand addPeerCommand)
		{
			var peer = new RaftPeer
			{
				Name = addPeerCommand.Name,
				LastKnownIndex = 0,
				LastSeen = DateTime.UtcNow
			};
			_peers.Add(addPeerCommand.Name, peer);

			_log.AddPeer(peer);

		    AnnounceCandidacy();
		}

	    
	    private void Handle(AppendCommand e)
		{
			if (State != RaftEngineState.Leader)
			{
				if (e.CompletionSource != null)
					e.CompletionSource.TrySetException(new InvalidOperationException("Not a leader"));
				return;
			}

			CurrentIndex += 1;
			var entry = new EntryId(CurrentTerm, CurrentIndex);

			if (e.CompletionSource != null)
				_pending.Add(entry, e.CompletionSource);

	        _log.AddEntry(entry, e.Stream);

			if (_peers.Count == 0)
			{
				_log.ApplyCommit(entry, OnCommitEntry);
				return;
			}


		

			foreach (var peer in _peers.Keys)
			{
				Transport.Send(peer, new AppendEntriesRequest
				{
					Term = CurrentTerm,
                    Commit = _log.Commit,
					Leader = Name,
                    PrevLog = _log.GetLastLogEntry() ?? new EntryId(),
					Entries = new[]
				{
					new AppendEntry
					{
						Entry = entry,
						Stream = _log.ReadEntry(entry)
					}
				}
				});
			}
		}

		private void AnnounceCandidacy()
		{
			if (State == RaftEngineState.Candidate && _hungElectionTimeoutMilliseconds == null)
			{
				// we have a hung election, will wait for a random period of time
				_hungElectionTimeoutMilliseconds = _rnd.Next(_electionTimeoutMilliseconds / 4, _electionTimeoutMilliseconds / 2);
				return;
			}

			State = RaftEngineState.Candidate;
			CurrentTerm++;
			Log.WriteLine("Calling an election in term {0}, voting for myself: {1}", CurrentTerm, Name);
			VotedFor = Name;
			VotesForMe = 1;

            var lastEntry = _log.GetLastLogEntry() ?? new EntryId();
			var rvr = new RequestVoteRequest
			{
				Term = CurrentTerm,
				Candidate = Name,
				LastLogIndex = lastEntry.Index,
				LastLogTerm = lastEntry.Term
			};
			foreach (var peer in _peers.Keys)
			{
				Transport.Send(peer, rvr);
			}
		}



		public ITransport Transport { get; private set; }

		public int QuoromSize
		{
			get { return _peers.Count / 2 + 1; }
		}

		public RaftEngineState State
		{
			get { return _state; }
			set
			{
				var prevState = _state;
				_state = value;
				_hungElectionTimeoutMilliseconds = null;
				if (_state == RaftEngineState.Leader)
				{
					CurrentLeader = Name;
					foreach (var peer in _peers.Values)
					{
						peer.LastKnownIndex = 0;
						peer.LastSeen = DateTime.MinValue;
					}
				}
				else
				{
					foreach (var tcs in _pending.Values)
					{
						tcs.TrySetCanceled();
					}
					_pending.Clear();
				}
				var onStateChanged = StateChanged;
				if (onStateChanged != null && prevState != _state)
				{
					onStateChanged(this, new StateChangedEventArges
					{
						State = _state,
						PreviousState = prevState
					});
				}
			}
		}

		public string Name { get; private set; }

		public long CurrentTerm
		{
			get { return _currentTerm; }
			set
			{
				var prevTerm = _currentTerm;
				_currentTerm = value;
				VotedFor = null;
				VotesForMe = 0;
				var onTermChanged = TermChanged;
				if (onTermChanged != null && _currentTerm != prevTerm)
				{
					onTermChanged(this, new TermChangedEventArgs
					{
						CurrentTerm = _currentTerm,
						PreviousTerm = prevTerm
					});
				}
			}
		}

		public string CurrentLeader
		{
			get { return _currentLeader; }
			set
			{
				var prevLeader = _currentLeader;
				_currentLeader = value;

				var onLeaderChanged = LeaderChanged;
				if (onLeaderChanged != null && prevLeader != _currentLeader)
				{
					onLeaderChanged(this, new LeaderChangedEventArgs
					{
						Leader = _currentLeader,
						PreviousLeader = prevLeader
					});
				}
			}
		}

		public string VotedFor { get; set; }


		public long CurrentIndex { get; set; }

		public int VotesForMe { get; set; }

		public void Dispose()
		{
            Log.WriteLine("Disposing {0}", Name);
			_cancellationTokenSource.Cancel();

			foreach (var tcs in _pending)
			{
				tcs.Value.TrySetCanceled();
			}
			_pending.Clear();

			if (_eventLoopTask != null)
			{
				try
				{
					_eventLoopTask.Wait();
				}
				catch (OperationCanceledException)
				{
				}
				catch (AggregateException ae)
				{
					if (ae.InnerException is OperationCanceledException == false)
						throw;
				}
			}
            _events.Dispose();
            if (_log != null)
                _log.Dispose();
		}

		public event EventHandler<CommitEntryEventArges> CommitEntry;
		public event EventHandler<LeaderChangedEventArgs> LeaderChanged;
		public event EventHandler<TermChangedEventArgs> TermChanged;
		public event EventHandler<StateChangedEventArges> StateChanged;



		public AppendEntriesResponse Handle(AppendEntriesRequest req)
		{
			if (req.Term < CurrentTerm)
			{
				Log.WriteLine("Rejecting append entries request from {0} because term {1} is smaller than current term {2}", req.Leader, req.Term, CurrentTerm);
				return new AppendEntriesResponse
				{
					Success = false,
					Term = CurrentTerm,
					Entry = new EntryId(CurrentTerm, CurrentIndex),
                    Commit = _log.Commit,
				};
			}

			if (req.Term == CurrentTerm)
			{
				if (State == RaftEngineState.Leader)
				{
					Log.WriteLine("Another node ({0}) attempted to append entries for a term ({1}) this node is already the leader", req.Leader, req.Term); 
					throw new InvalidOperationException("Another leader elected for the same term: " + req.Term);
				}

				CurrentLeader = req.Leader;
				State = RaftEngineState.Follower;
			}
			else
			{
				UpdateCurrentTerm(req.Term, req.Leader);
			}

            if (_log.TruncateLogAfter(req.PrevLog) == false)
			{
				Log.WriteLine("The previous log {1} for append entries request from {0} is _behind_ our log", req.Leader, req.PrevLog);
				return new AppendEntriesResponse
				{
					Success = false,
                    Commit = _log.Commit,
					Entry = new EntryId(CurrentTerm, CurrentIndex),
				};
			}

			var lastEntry = new EntryId();

			foreach (var appendEntry in req.Entries)
			{
				lastEntry = appendEntry.Entry;
                _log.AddEntry(appendEntry.Entry, appendEntry.Stream);
			}

			_log.ApplyCommit(req.Commit, OnCommitEntry);

			return new AppendEntriesResponse
			{
				Commit = _log.Commit,
				Entry = lastEntry,
				Success = true
			};
		}

		public void Handle(AppendEntriesResponse resp)
		{
			if (State != RaftEngineState.Leader)
				return; // only leaders can process such replies

			if (resp.Term > CurrentTerm) // there is a new leader in town, time to step down
			{
				UpdateCurrentTerm(resp.Term, null);
				return;
			}

			if (resp.Success == false)
			{
				// wiil be retried / replaced by another leader
				return;
			}

			RaftPeer peer;
			if (_peers.TryGetValue(resp.From, out peer) == false)
				return; // a response from an unknown peer, ignoring it

			peer.LastSeen = DateTime.UtcNow;
			peer.LastKnownIndex = resp.Entry.Index;

			var replicated = _peers.Select(x => x.Value.LastKnownIndex).OrderBy(x => x).ToList();
			var canCommit = replicated[QuoromSize];

			if (canCommit <= _log.Commit.Index)
				return; // we already go enough votes to commit it, so no need to do anything else

			_log.ApplyCommit(new EntryId(CurrentTerm, canCommit), OnCommitEntry);
		}

		public RequestVoteResponse Handle(RequestVoteRequest req)
		{
			if (req.Term < CurrentTerm)
			{
				Log.WriteLine("Requested vote by {1} for term {0} that is smaller than the current term {2}", req.Term, req.Candidate, CurrentTerm);
				return new RequestVoteResponse
				{
					Term = CurrentTerm,
					VoteGranted = false
				};
			}
			if (req.Term > CurrentTerm)
			{
				UpdateCurrentTerm(req.Term, null);
			}
			else if (VotedFor != null && VotedFor != req.Candidate)
			{
				Log.WriteLine("Requested vote by {0} for term {1} was rejected, already voted for {2} in this term", req.Candidate, req.Term, VotedFor);
				// already voted for someone else
				return new RequestVoteResponse
				{
					Term = CurrentTerm,
					VoteGranted = false
				};
			}

            var lastLogEntry = _log.GetLastLogEntry();
			if (lastLogEntry != null)
			{
				if (lastLogEntry.Value.Index > req.LastLogIndex || lastLogEntry.Value.Term > req.LastLogTerm)
				{
					Log.WriteLine("Requested vote by {0} for term {1} was rejected, candidate is missing last log entry {2}", req.Candidate, req.Term, lastLogEntry); 
					return new RequestVoteResponse
					{
						Term = CurrentTerm,
						VoteGranted = false
					};
				}
			}
			Log.WriteLine("Casting vote for {0} in term {1}", req.Candidate, req.Term);
			VotedFor = req.Candidate;
			return new RequestVoteResponse
			{
				Term = CurrentTerm,
				VoteGranted = true
			};
		}

		public void Handle(RequestVoteResponse resp)
		{
			if (resp.Term > CurrentTerm)
			{
				UpdateCurrentTerm(resp.Term, null);
			}
			else if (resp.VoteGranted)
			{
				if (State == RaftEngineState.Leader)
					return; // irrelevant

				VotesForMe += 1;
				if (VotesForMe < QuoromSize)
					return;
				Log.WriteLine("{0} was selected as leader", Name);
				State = RaftEngineState.Leader;

				Handle(new AppendCommand
				{
					Stream = new MemoryStream() // NOP command - materialize the leader - 5.2
				});
			}
		}

		private void UpdateCurrentTerm(long term, string leader)
		{
			if (CurrentTerm >= term)
				throw new InvalidOperationException("Cannot update the term to a term that isn't greater than the current term");

			CurrentLeader = leader;
			CurrentTerm = term;
			State = RaftEngineState.Follower;
			VotedFor = null;
		}


		protected void OnCommitEntry(EntryId entry, Stream stream)
		{
			var handler = CommitEntry;
			try
			{
				if (handler == null)
					throw new InvalidOperationException(
						"Attempted to commit entry, but there were no subscribers to the CommitEntry event");
				handler(this, new CommitEntryEventArges
				{
				    Entry = entry,
                    Stream = stream
				});
			}
			catch (Exception ex)
			{
				while (_pending.Count > 0)
				{
					var item = _pending.First();
                    if (item.Key > entry)
						break;

					item.Value.TrySetException(ex);
					_pending.Remove(item.Key);
				}
				throw;
			}
			while (_pending.Count > 0)
			{
				var item = _pending.First();
                if (item.Key > entry)
					break;

				item.Value.TrySetResult(null);
				_pending.Remove(item.Key);
			}

		}

		private class AppendCommand
		{
			public TaskCompletionSource<object> CompletionSource;
			public Stream Stream;
		}

		private class AddPeerCommand
		{
			public string Name;
		}

		private class RemvoePeerCommand
		{
			public string Name;
		}

	    internal void AddPeerInternal(RaftPeer peer)
	    {
	        _peers[peer.Name] = peer;
	    }
	}
}