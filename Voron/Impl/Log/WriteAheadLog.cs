// -----------------------------------------------------------------------
//  <copyright file="WriteAheadLog.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Collections.Generic;
using Voron.Trees;

namespace Voron.Impl.Log
{
	public class WriteAheadLog
	{
		private readonly FlushMode _logsFlushMode;
		private int _current = -1;
		private readonly List<LogFile> _logfiles = new List<LogFile>();

		public WriteAheadLog(FlushMode logsFlushMode = FlushMode.Full)
		{
			_logsFlushMode = logsFlushMode;
		}

		private void CreateNextFile()
		{
			_current++;
			var log = new LogFile(new MemoryMapPager(string.Format("{0:D10}.txlog", _current), _logsFlushMode));

			_logfiles.Add(log);
		}

		public void TransactionBegin()
		{
			if(_logfiles.Count == 0)
				CreateNextFile();

			_logfiles[_current].TransactionBegin();
		}

		public Page GetPage(Transaction tx, long pageNumber)
		{
			for (int i = _logfiles.Count - 1; i >= 0; i--)
			{
				var page = _logfiles[i].GetPage(tx, pageNumber);
				if (page != null)
					return page;
			}

			return null;
		}
	}
}