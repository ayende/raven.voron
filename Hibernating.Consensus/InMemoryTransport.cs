using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using Hibernating.Consensus.Messages;
using Newtonsoft.Json;

namespace Hibernating.Consensus
{
	public class InMemoryTransport : ITransport
	{
        public static TextWriter Log = Console.Out;
		private readonly string _name;

		private readonly Dictionary<string, BlockingCollection<Envelope>> _dests =
			new Dictionary<string, BlockingCollection<Envelope>>();

		public InMemoryTransport(string name)
		{
			_name = name;
		}

		public void SetSink(BlockingCollection<Envelope> sink)
		{
			_dests[_name] = sink;
		}

		public void Send(string name, RequestVoteRequest req)
		{
			Send(name, (object) req);
		}

		public void Send(string name, AppendEntriesRequest req)
		{
			Send(name, (object)req);
		}

		public void Send(string name, AppendEntriesResponse resp)
		{
			Send(name, (object)resp);
		}

		public void Send(string name, ErrorReportEvent resp)
		{
			Send(name, (object)resp);
		}

		public void Send(string name, RequestVoteResponse resp)
		{
			Send(name, (object)resp);
		}

		private void Send(string name, object msg)
		{
            Log.WriteLine(_name + " -> " + name + " " + msg.GetType().Name + " " + JsonConvert.SerializeObject(msg));
		    try
		    {
		        _dests[name].Add(new Envelope
		        {
		            Message = msg,
		            Source = _name
		        });
		    }
		    catch (ObjectDisposedException)
		    {
		    }
		}

		public void BidirectionalConnectionTo(InMemoryTransport other)
		{
			_dests.Add(other._name, other._dests[other._name]);
			other._dests.Add(_name, _dests[_name]);
		}
	}
}