using System;
using System.IO;
using Voron.Events;
using Voron.Headers;
using Voron.Impl;
using Voron.Journal;
using Voron.Pagers;
using Voron.Utilities;

namespace Voron
{
	public abstract class StorageEnvironmentOptions : IDisposable
	{
		public event EventHandler<RecoveryErrorEventArgs> OnRecoveryError;

		public void InvokeRecoveryError(object sender, string message, Exception e)
		{
			var handler = OnRecoveryError;
			if (handler == null)
			{
				throw new InvalidDataException(message + Environment.NewLine +
					 "An exception has been thrown because there isn't a listener to the OnRecoveryError event on the storage options.", e);
			}

			handler(this, new RecoveryErrorEventArgs(message, e));
		}

		public Action<long> OnScratchBufferSizeChanged = delegate { };

		public long? InitialFileSize { get; set; }

        public int PageSize { get; set; }

		public long MaxLogFileSize
		{
			get { return _maxLogFileSize; }
			set
			{
				if (value < _initialLogFileSize)
					InitialLogFileSize = value;
				_maxLogFileSize = value;
			}
		}

		public long InitialLogFileSize
		{
			get { return _initialLogFileSize; }
			set
			{
				if (value > MaxLogFileSize)
					MaxLogFileSize = value;
				_initialLogFileSize = value;
			}
		}

		public long MaxScratchBufferSize { get; set; }

		public bool OwnsPagers { get; set; }

		public bool ManualFlushing { get; set; }

		public bool IncrementalBackupEnabled { get; set; }

		public abstract IVirtualPager DataPager { get; }

		public long MaxNumberOfPagesInJournalBeforeFlush { get; set; }

		public int IdleFlushTimeout { get; set; }

		public long? MaxStorageSize { get; set; }
		public abstract string BasePath { get; }

		public abstract IJournalWriter CreateJournalWriter(long journalNumber, long journalSize);

		protected bool Disposed;
		private long _initialLogFileSize;
		private long _maxLogFileSize;

		protected StorageEnvironmentOptions()
		{
		    PageSize = 4096; // 4K

			MaxNumberOfPagesInJournalBeforeFlush = 1024; // 4 MB if 4K

			IdleFlushTimeout = 5000; // 5 seconds

			MaxLogFileSize = 64 * 1024 * 1024;

			InitialLogFileSize = 64 * 1024;

			MaxScratchBufferSize = 512 * 1024 * 1024;

			ScratchBufferOverflowTimeout = 5000;

			OwnsPagers = true;

			IncrementalBackupEnabled = false;
		}

		public int ScratchBufferOverflowTimeout { get; set; }

		public static StorageEnvironmentOptions CreateMemoryOnly()
		{
			return new PureMemoryStorageEnvironmentOptions();
		}

		public static StorageEnvironmentOptions ForPath(string path, string tempPath = null, string journalPath = null)
		{
			return new DirectoryStorageEnvironmentOptions(path, tempPath, journalPath);
		}

		public IDisposable AllowManualFlushing()
		{
			var old = ManualFlushing;
			ManualFlushing = true;

			return new DisposableAction(() => ManualFlushing = old);
		}


		public static string JournalName(long number)
		{
			return string.Format("{0:D19}.journal", number);
		}

		public static string JournalRecoveryName(long number)
		{
			return string.Format("{0:D19}.recovery", number);
		}

		public static string ScratchBufferName(long number)
		{
			return string.Format("scratch.{0:D10}.buffers", number);
		}

		public abstract void Dispose();

		public abstract bool TryDeleteJournal(long number);

		public unsafe abstract bool ReadHeader(string filename, FileHeader* header);

		public unsafe abstract void WriteHeader(string filename, FileHeader* header);

		public abstract IVirtualPager CreateScratchPager(string name);

		public abstract IVirtualPager OpenJournalPager(long journalNumber);

        public abstract IVirtualPager OpenPager(string filename);

		public static bool RunningOnPosix
		{
			get
			{
				switch (Environment.OSVersion.Platform)
				{
					case PlatformID.Win32S:
					case PlatformID.Win32Windows:
					case PlatformID.Win32NT:
					case PlatformID.WinCE:
					case PlatformID.Xbox:
						return false;
					case PlatformID.Unix:
					case PlatformID.MacOSX:
						return true;
					default:
						return false; // we'll try the windows version here
				}
			}
		}
	}
}
