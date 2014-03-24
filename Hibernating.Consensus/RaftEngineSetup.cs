using System;
using System.Collections.Generic;
using Voron;

namespace Hibernating.Consensus
{
	public class RaftEngineSetup
	{
		public string Name { get; set; }

		public int ElectionTimeoutMilliseconds { get; set; }

		public ITransport Transport { get; set; }

		public StorageEnvironmentOptions Options { get; set; }

		public List<string> Peers { get; private set; }

		public RaftEngineSetup()
		{
			Peers = new List<string>();
		}

		public void Validate()
		{
			if (string.IsNullOrEmpty(Name))
				throw new ArgumentException("The name cannot be null or empty");
			if (Transport == null)
				throw new ArgumentException("The transport cannot be null");

			if (ElectionTimeoutMilliseconds == 0 || ElectionTimeoutMilliseconds / 4 == 0)
				throw new ArgumentException("Election timeout must have a value greater than 4");

			if (Options == null)
				throw new ArgumentException("Voron's storage option must be specified");
		}
	}
}