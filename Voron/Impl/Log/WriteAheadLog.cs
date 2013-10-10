// -----------------------------------------------------------------------
//  <copyright file="WriteAheadLog.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
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

		public WriteAheadLog(StorageEnvironment env, Func<string, IVirtualPager> createLogFilePager, IVirtualPager dataPager)
		{
			_env = env;
			_createLogFilePager = createLogFilePager;
			_dataPager = dataPager;
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

			WriteLogState();

			return log;
		}

		public void Recover()
		{
			var info = ReadLogInfo();

			for (var logNumber = info->RecentLog - info->LogFilesCount + 1; logNumber <= info->RecentLog; logNumber++)
			{
				var log = new LogFile(_createLogFilePager(_logName(logNumber)), logNumber);

				_logFiles.Add(log);
			}

			_currentLogNumber = info->RecentLog;

			// TODO need to recover by using log files
		}

		private LogInfo* ReadLogInfo()
		{
			var logInfoPage = _dataPager.Read(null, 0);
			var logInfo = (LogInfo*)logInfoPage.Base + Constants.PageHeaderSize;

			return logInfo;
		}

		public void WriteLogState(long pageNumber = 0)
		{
			var logInfoPage = _dataPager.TempPage;
			logInfoPage.PageNumber = pageNumber;
			var logInfo = (LogInfo*) logInfoPage.Base + Constants.PageHeaderSize;

			logInfo->RecentLog = _currentFile != null ? _currentFile.Number : -1;
			logInfo->LogFilesCount = _logFiles.Count;

			_dataPager.Write(logInfoPage);
			_dataPager.Sync();
		}

		public void WriteFileHeader()
		{
			var fileHeaderPage = _dataPager.TempPage;
			fileHeaderPage.PageNumber = 1;// TODO 

			var fileHeader = ((FileHeader*)fileHeaderPage.Base + Constants.PageHeaderSize);

			fileHeader->MagicMarker = Constants.MagicMarker;
			fileHeader->Version = Constants.CurrentVersion;
			fileHeader->TransactionId = _currentFile.LastCommittedTransactionId;
			fileHeader->LastPageNumber = _currentFile.LastPageNumberOfCommittedTransaction;
			_env.FreeSpaceHandling.CopyStateTo(&fileHeader->FreeSpace);
			_env.Root.State.CopyTo(&fileHeader->Root);

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
				logFile.Flush(); //TODO need to do it better - no need to flush all log files
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

			WriteFileHeader();

			_logFiles.RemoveRange(0, _logFiles.Count - 1);

			Debug.Assert( _logFiles.Count == 1 && _currentFile == _logFiles.Last());

			if (_currentFile.AvailablePages < 2)
			{
				_logFiles.Clear();
				_currentFile = null;
			}

			WriteLogState();
		}

		public void Dispose()
		{
			foreach (var logFile in _logFiles)
			{
				logFile.Dispose();
			}

			_logFiles.Clear();
		}
	}
}