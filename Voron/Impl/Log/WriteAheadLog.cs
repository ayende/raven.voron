// -----------------------------------------------------------------------
//  <copyright file="WriteAheadLog.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using Voron.Impl.FileHeaders;
using Voron.Trees;
using Voron.Util;

namespace Voron.Impl.Log
{
	public unsafe class WriteAheadLog : IDisposable
	{
		private readonly StorageEnvironment _env;
		private readonly Func<string, IVirtualPager> _createLogFilePager;
		private readonly IVirtualPager _dataPager;
		private readonly bool _deleteUnusedLogFiles;
		private readonly Func<long, string> _logName = number => string.Format("{0:D19}.txlog", number);
		private readonly bool _disposeLogFiles;

		private LogFile _splitLogFile;
		private long _logIndex = -1;
		private FileHeader* _fileHeader;
		private IntPtr _inMemoryHeader;
		private long _dataFlushCounter = 0;
		private bool _disabled;

		internal ImmutableList<LogFile> Files = ImmutableList<LogFile>.Empty;
		internal LogFile CurrentFile;

		public WriteAheadLog(StorageEnvironment env, Func<string, IVirtualPager> createLogFilePager, IVirtualPager dataPager,
		                     long logFileSize, bool deleteUnusedLogFiles, bool disposeLogFiles)
		{
			_env = env;
			_createLogFilePager = createLogFilePager;
			_dataPager = dataPager;
			_deleteUnusedLogFiles = deleteUnusedLogFiles;
			_disposeLogFiles = disposeLogFiles;
			LogFileSize = logFileSize;
			_fileHeader = GetEmptyFileHeader();
		}

		public long LogFileSize { get; private set; }

		private LogFile NextFile(Transaction tx)
		{
			_logIndex++;

			var logPager = _createLogFilePager(_logName(_logIndex));

			logPager.AllocateMorePages(null, LogFileSize);

			var log = new LogFile(logPager, _logIndex);
			log.AddRef(); // one reference added by a creator - write ahead log
			tx.SetLogReference(log); // and the next one for the current transaction

			Files = Files.Add(log);

			UpdateLogInfo();
			WriteFileHeader();

			return log;
		}

		public bool TryRecover(FileHeader* fileHeader, out TransactionHeader* lastTxHeader)
		{
			_fileHeader = fileHeader;
			var logInfo = fileHeader->LogInfo;

			lastTxHeader = null;

			if (logInfo.LogFilesCount == 0)
			{
				return false;
			}

			for (var logNumber = logInfo.RecentLog - logInfo.LogFilesCount + 1; logNumber <= logInfo.RecentLog; logNumber++)
			{
				var pager = _createLogFilePager(_logName(logNumber));

				if (pager.NumberOfAllocatedPages != (LogFileSize/pager.PageSize))
					throw new InvalidDataException("Log file " + _logName(logNumber) + " should contain " +
					                               (LogFileSize/pager.PageSize) + " pages, while it has " +
					                               pager.NumberOfAllocatedPages + " pages allocated.");
				var log = new LogFile(pager, logNumber);
				log.AddRef(); // creator reference - write ahead log
				Files = Files.Add(log);
			}

			foreach (var logItem in Files)
			{
				long startRead = 0;

				if (logItem.Number == logInfo.LastSyncedLog)
					startRead = logInfo.LastSyncedLogPage + 1;

				lastTxHeader = logItem.RecoverAndValidate(startRead, lastTxHeader);
			}

			_logIndex = logInfo.RecentLog;
			_dataFlushCounter = logInfo.DataFlushCounter + 1;

			return true;
		}

		public void UpdateLogInfo()
		{
			_fileHeader->LogInfo.RecentLog = Files.Count > 0 ? _logIndex : -1;
			_fileHeader->LogInfo.LogFilesCount = Files.Count;
			_fileHeader->LogInfo.DataFlushCounter = _dataFlushCounter;
		}

		public void UpdateFileHeaderAfterDataFileSync(CommitPoint commitPoint)
		{
			_fileHeader->TransactionId = commitPoint.TxId;
			_fileHeader->LastPageNumber = commitPoint.TxLastPageNumber;

			_fileHeader->LogInfo.LastSyncedLog = commitPoint.LogNumber;
			_fileHeader->LogInfo.LastSyncedLogPage = commitPoint.LastWrittenLogPage;
			_fileHeader->LogInfo.DataFlushCounter = _dataFlushCounter;

			_env.FreeSpaceHandling.CopyStateTo(&_fileHeader->FreeSpace);
			_env.Root.State.CopyTo(&_fileHeader->Root);
		}

		internal void WriteFileHeader(long? pageToWriteHeader = null)
		{
			var fileHeaderPage = _dataPager.TempPage;

			if (pageToWriteHeader == null)
				fileHeaderPage.PageNumber = _dataFlushCounter & 1;
			else
				fileHeaderPage.PageNumber = pageToWriteHeader.Value;

			var header = ((FileHeader*)fileHeaderPage.Base + Constants.PageHeaderSize);

			header->MagicMarker = Constants.MagicMarker;
			header->Version = Constants.CurrentVersion;
			header->TransactionId = _fileHeader->TransactionId;
			header->LastPageNumber = _fileHeader->LastPageNumber;
			header->FreeSpace = _fileHeader->FreeSpace;
			header->LogInfo = _fileHeader->LogInfo;
			header->Root = _fileHeader->Root;

			_dataPager.Write(fileHeaderPage);
			_dataPager.Sync();
		}

		public void TransactionBegin(Transaction tx)
		{
			if(_disabled)
				return;
			
			if (CurrentFile == null)
				CurrentFile = NextFile(tx);

			if (_splitLogFile != null) // last split transaction was not committed
			{
				Debug.Assert(_splitLogFile.LastTransactionCommitted == false);
				CurrentFile = _splitLogFile;
				_splitLogFile = null;
			}

			CurrentFile.TransactionBegin(tx);
		}

		public void TransactionCommit(Transaction tx)
		{
			if(_disabled)
				return;

			if (_splitLogFile != null)
			{
				_splitLogFile.TransactionCommit(tx);
				_splitLogFile = null;
			}

			CurrentFile.TransactionCommit(tx);

			if (CurrentFile.AvailablePages < 2) // it must have at least one page for the next transaction header and one page for data
			{
				CurrentFile = null; // it will force new log file creation when next transaction will start
			}
		}

		public Page ReadPage(Transaction tx, long pageNumber)
		{
			// read transactions have to read from log snapshots
			if (tx.Flags == TransactionFlags.Read)
			{
				// read log snapshots from the back to get the most recent version of a page
				for (var i = tx.LogSnapshots.Count -1; i >= 0; i--)
				{
					var page = tx.LogSnapshots[i].ReadPage(pageNumber);
					if (page != null)
						return page;
				}

				return null;
			}

			// write transactions can read directly from logs
			var logs = Files; // thread safety copy

			for (var i = logs.Count - 1; i >= 0; i--)
			{
				var page = logs[i].ReadPage(tx, pageNumber);
				if (page != null)
					return page;
			}

			return null;
		}

		public Page Allocate(Transaction tx, long startPage, int numberOfPages)
		{
			if (CurrentFile.AvailablePages < numberOfPages)
			{
				if (_splitLogFile != null) // we are already in a split transaction and don't allow to spread a transaction over more than two log files
					throw new InvalidOperationException(
						"Transaction attempted to put data in more than two log files. It's not allowed. The transaction is too large.");

				// here we need to mark that transaction is split in both log files
				// it will have th following transaction markers in the headers
				// log_1: [Start|Split] log_2: [Split|Commit]

				CurrentFile.TransactionSplit(tx);
				_splitLogFile = CurrentFile;

				CurrentFile = NextFile(tx);

				CurrentFile.TransactionSplit(tx);
			}

			return CurrentFile.Allocate(startPage, numberOfPages);
		}

		public void ApplyLogsToDataFile()
		{
			if(Files.Count == 0)
				return;

			var processingLogs = Files; // thread safety copy

			var lastSyncedLog = _fileHeader->LogInfo.LastSyncedLog;
			var lastSyncedPage = _fileHeader->LogInfo.LastSyncedLogPage;

			Debug.Assert(processingLogs.First().Number >= lastSyncedLog);

			var pagesToWrite = new Dictionary<long, Page>();

			// read from the end in order to write only the most recent version of a page
			var recentLogIndex = processingLogs.Count - 1;
			var recentLogCommit = processingLogs[recentLogIndex].LastCommit;

			for (var i = recentLogIndex; i >= 0; i--)
			{
				var log = processingLogs[i];

				foreach (var pageNumber in log.GetModifiedPages(log.Number == lastSyncedLog ? lastSyncedPage : (long?) null))
				{
					if (pagesToWrite.ContainsKey(pageNumber) == false)
					{
						pagesToWrite[pageNumber] = log.ReadPage(null, pageNumber);
					}
				}
			}

			var sortedPages = pagesToWrite.OrderBy(x => x.Key).Select(x => x.Value).ToList();

			if(sortedPages.Count == 0)
				return;

			var last = sortedPages.Last();

			_dataPager.EnsureContinuous(null, last.PageNumber, last.IsOverflow ? Page.GetNumberOfOverflowPages(_dataPager.PageSize, last.OverflowSize) : 1);

			foreach (var page in sortedPages)
			{
				_dataPager.Write(page);
			}

			_dataPager.Sync();

			UpdateFileHeaderAfterDataFileSync(recentLogCommit);

			var fullLogs = processingLogs.GetRange(0, recentLogIndex);

			foreach (var fullLog in fullLogs)
			{
				if (_deleteUnusedLogFiles)
					fullLog.DeleteOnClose();
			}

			UpdateLogInfo();

			Files = Files.RemoveRange(0, recentLogIndex);

			foreach (var fullLog in fullLogs)
			{
				fullLog.Release();
			}

			if (Files.Count == 0)
				CurrentFile = null;

			WriteFileHeader();

			_dataFlushCounter++;
		}

		public void Dispose()
		{
			if (_inMemoryHeader != IntPtr.Zero)
			{
				Marshal.FreeHGlobal(_inMemoryHeader);
				_inMemoryHeader = IntPtr.Zero;
			}

			if(_disposeLogFiles)
			{
				foreach (var logFile in Files)
				{
					logFile.Dispose();
				}
			}

			Files.Clear();
		}

		private FileHeader* GetEmptyFileHeader()
		{
			if(_inMemoryHeader == IntPtr.Zero)
				_inMemoryHeader = Marshal.AllocHGlobal(_dataPager.PageSize);

			var header = (FileHeader*) _inMemoryHeader;

			header->MagicMarker = Constants.MagicMarker;
			header->Version = Constants.CurrentVersion;
			header->TransactionId = 0;
			header->LastPageNumber = 1;
			header->FreeSpace.FirstBufferPageNumber = -1;
			header->FreeSpace.SecondBufferPageNumber = -1;
			header->FreeSpace.NumberOfTrackedPages = 0;
			header->FreeSpace.NumberOfPagesTakenForTracking = 0;
			header->FreeSpace.PageSize = -1;
			header->FreeSpace.Checksum = 0;
			header->Root.RootPageNumber = -1;
			header->LogInfo.DataFlushCounter = -1;
			header->LogInfo.RecentLog = -1;
			header->LogInfo.LogFilesCount = 0;
			header->LogInfo.LastSyncedLog = -1;
			header->LogInfo.LastSyncedLogPage = -1;

			return header;
		}

		public IDisposable DisableLog()
		{
			_disabled = true;
			return new DisposableAction(() => _disabled = false);
		}

		public List<LogSnapshot> GetSnapshots()
		{
			return Files.Select(x => x.GetSnapshot()).ToList();
		} 
	}
}