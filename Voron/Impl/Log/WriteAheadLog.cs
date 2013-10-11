// -----------------------------------------------------------------------
//  <copyright file="WriteAheadLog.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.InteropServices;
using Voron.Impl.FileHeaders;
using Voron.Trees;

namespace Voron.Impl.Log
{
	public unsafe class WriteAheadLog : IDisposable
	{
		private readonly StorageEnvironment _env;
		private readonly Func<string, IVirtualPager> _createLogFilePager;
		private readonly IVirtualPager _dataPager;
		private long _currentLogNumber = -1;
		private LogFile _currentFile;
		private readonly List<LogFile> _logFiles = new List<LogFile>();
		private readonly Func<long, string> _logName = number => string.Format("{0:D19}.txlog", number);
		private FileHeader* _fileHeader;
		private IntPtr _inMemoryHeader;
		private bool _disposeLogFiles;

		public WriteAheadLog(StorageEnvironment env, Func<string, IVirtualPager> createLogFilePager, IVirtualPager dataPager, bool disposeLogFiles = true)
		{
			_env = env;
			_createLogFilePager = createLogFilePager;
			_dataPager = dataPager;
			_disposeLogFiles = disposeLogFiles;
			_fileHeader = GetEmptyFileHeader();
		}

		public long LogFileSize
		{
			get { return 64*1024*1024; }
		}

		private LogFile NextFile()
		{
			_currentLogNumber++;

			var logPager = _createLogFilePager(_logName(_currentLogNumber));
			logPager.AllocateMorePages(null, LogFileSize);

			var log = new LogFile(logPager, _currentLogNumber);

			_logFiles.Add(log);

			UpdateLogInfo();
			WriteFileHeader();

			return log;
		}

		public void Recovery(FileHeader* fileHeader)
		{
			_fileHeader = fileHeader;
			var logInfo = fileHeader->LogInfo;

			for (var logNumber = logInfo.RecentLog - logInfo.LogFilesCount + 1; logNumber <= logInfo.RecentLog; logNumber++)
			{
				_logFiles.Add(new LogFile(_createLogFilePager(_logName(logNumber)), logNumber));
			}

			foreach (var logFile in _logFiles)
			{
				logFile.BuildPageTranslationTable();
			}

			_currentLogNumber = logInfo.RecentLog;
			_currentFile = _logFiles.Last();
		}

		public void UpdateLogInfo()
		{
			_fileHeader->LogInfo.RecentLog = _logFiles.Count > 0 ? _logFiles.Last().Number : -1;
			_fileHeader->LogInfo.LogFilesCount = _logFiles.Count;
		}

		public void UpdateFileHeaderAfterDataFileSync()
		{
			_fileHeader->TransactionId = _currentFile.LastCommittedTransactionId;
			_fileHeader->LastPageNumber = _currentFile.LastPageNumberOfLastCommittedTransaction;

			_fileHeader->LogInfo.LastSyncedLog = _currentFile.Number;
			_fileHeader->LogInfo.LastSyncedPage = _currentFile.LastSyncedPage;

			_env.FreeSpaceHandling.CopyStateTo(&_fileHeader->FreeSpace);
			_env.Root.State.CopyTo(&_fileHeader->Root);
		}

		private void WriteFileHeader()
		{
			var fileHeaderPage = _dataPager.TempPage;
			fileHeaderPage.PageNumber = 0;// TODO 

			var header = ((FileHeader*)fileHeaderPage.Base + Constants.PageHeaderSize);

			header->MagicMarker = Constants.MagicMarker;
			header->Version = Constants.CurrentVersion;
			header->TransactionId = _fileHeader->TransactionId;
			header->LastPageNumber = _fileHeader->LastPageNumber;
			header->FreeSpace = _fileHeader->FreeSpace;
			header->LogInfo = _fileHeader->LogInfo;
			header->Root = _fileHeader->Root;

			_dataPager.EnsureContinuous(null, fileHeaderPage.PageNumber, 1);

			_dataPager.Write(fileHeaderPage);
			_dataPager.Sync();
		}

		public void TransactionBegin(Transaction tx)
		{
			if(_currentFile == null)
				_currentFile = NextFile();

			_currentFile.TransactionBegin(tx);
		}

		public void TransactionCommit(Transaction tx)
		{
			_currentFile.TransactionCommit(tx);
		}

		public Page ReadPage(Transaction tx, long pageNumber)
		{
			for (var i = _logFiles.Count - 1; i >= 0; i--)
			{
				var page = _logFiles[i].ReadPage(tx, pageNumber);
				if (page != null)
					return page;
			}

			return null;
		}

		public Page Allocate(Transaction tx, long startPage, int numberOfPages)
		{
			if (_currentFile.AvailablePages < numberOfPages)
			{
				_currentFile.TransactionSplit(tx);
				_currentFile = NextFile();
				_currentFile.TransactionSplit(tx);
			}

			return _currentFile.Allocate(startPage, numberOfPages);
		}

		public void Flush()
		{
			foreach (var logFile in _logFiles)
			{
				logFile.Sync(); //TODO need to do it better - no need to flush all log files
			}
		}

		public void ApplyLogsToDataFile()
		{
			var pagesToWrite = new Dictionary<long, Page>();

			for (int i = _logFiles.Count - 1; i >= 0; i--)
			{
				foreach (var pageNumber in _logFiles[i].Pages)
				{
					if (pagesToWrite.ContainsKey(pageNumber) == false)
					{
						pagesToWrite[pageNumber] = _logFiles[i].ReadPage(null, pageNumber);
					}
				}
			}

			var sortedPages = pagesToWrite.OrderBy(x => x.Key).Select(x => x.Value).ToList();

			var last = sortedPages.LastOrDefault();

			if (last != null)
			{
				_dataPager.EnsureContinuous(null, last.PageNumber, 1);
			}

			foreach (var page in sortedPages)
			{
				_dataPager.Write(page);
			}

			_dataPager.Sync();

			UpdateFileHeaderAfterDataFileSync();

			for (int i = 0; i < _logFiles.Count -1; i++)
			{
				_logFiles[i].Dispose();
			}
			_logFiles.RemoveRange(0, _logFiles.Count - 1);

			Debug.Assert(_logFiles.Count == 1 && _currentFile == _logFiles.Last());

			if (_currentFile.AvailablePages < 2)
			{
				_currentFile.Dispose();
				_logFiles.Clear();
				_currentFile = null;
			}

			UpdateLogInfo();

			WriteFileHeader();
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
				foreach (var logFile in _logFiles)
				{
					logFile.Dispose();
				}
			}

			_logFiles.Clear();
		}

		private FileHeader* GetEmptyFileHeader()
		{
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
			header->LogInfo.RecentLog = -1;
			header->LogInfo.LogFilesCount = 0;
			header->LogInfo.LastSyncedLog = -1;
			header->LogInfo.LastSyncedPage = -1;

			return header;
		}
	}
}