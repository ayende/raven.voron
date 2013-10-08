// -----------------------------------------------------------------------
//  <copyright file="WriteAheadLog.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using Voron.Trees;

namespace Voron.Impl.Log
{
	public class WriteAheadLog : IDisposable
	{
		private readonly FlushMode _logsFlushMode;
		private int _fileIndex = -1;
		private LogFile _currentFile = null;
		private readonly List<LogFile> _logFiles = new List<LogFile>();

		public WriteAheadLog(FlushMode logsFlushMode = FlushMode.Full)
		{
			_logsFlushMode = logsFlushMode;

			_currentFile = CreateNextFile();
		}

		private LogFile CreateNextFile()
		{
			_fileIndex++;

			var logPager = new MemoryMapPager(string.Format("{0:D10}.txlog", _fileIndex), _logsFlushMode);
			logPager.AllocateMorePages(null, 64 * 1024 * 1024);

			var log = new LogFile(logPager);
			
			_logFiles.Add(log);

			return log;
		}

		public void TransactionBegin(Transaction tx)
		{
			_currentFile.TransactionBegin(tx);
		}

		public void TransactionCommit(Transaction tx)
		{
			_currentFile.TransactionCommit();
		}

		public Page GetPage(Transaction tx, long pageNumber)
		{
			for (var i = _logFiles.Count - 1; i >= 0; i--)
			{
				var page = _logFiles[i].GetPage(tx, pageNumber);
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
				_currentFile = CreateNextFile();
				_currentFile.TransactionSplit(tx);
			}

			return _currentFile.Allocate(tx, startPage, numberOfPages);
		}

		public void Flush()
		{
			foreach (var logFile in _logFiles)
			{
				logFile.Flush(); //TODO need to do it better - no need to flush all log files
			}
		}

		public void Dispose()
		{
			foreach (var logFile in _logFiles)
			{
				logFile.Dispose();
			}
		}
	}
}