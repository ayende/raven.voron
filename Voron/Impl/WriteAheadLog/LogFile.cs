// -----------------------------------------------------------------------
//  <copyright file="LogFile.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Diagnostics;
using Voron.Impl.FreeSpace;
using Voron.Trees;
using Voron.Util;

namespace Voron.Impl.WriteAheadLog
{
	public unsafe class LogFile : IDisposable
	{
		private readonly IVirtualPager _pager;
		private readonly Dictionary<long, long> _pageTranslationTable = new Dictionary<long, long>(); // TODO after restart we need to recover those values
		private long _writePage;//TODO after restart we need to recover this value
		private long _lastFlushedPage = -1;

		public LogFile(IVirtualPager pager)
		{
			_pager = pager;
			_writePage = 0;
		}

		public IVirtualPager Pager
		{
			get { return _pager; }
		}

		public void TransactionBegin()
		{
			if (_lastFlushedPage != _writePage - 1)
				_writePage = _lastFlushedPage + 1;

			var page = Allocate(_writePage, 1);

			var header = (LogEntryHeader*) page.Base;
		}

		public void Flush()
		{
			var start = _lastFlushedPage + 1;
			var count = _writePage - start;

			_pager.Flush(start, count);
			_pager.Sync();

			_lastFlushedPage += count;
		}

		public Page GetPage(Transaction tx, long pageNumber)
		{
			if (_pageTranslationTable.ContainsKey(pageNumber) == false)
				return null;

			return _pager.Get(tx, _pageTranslationTable[pageNumber]);
		}

		public Page Allocate(long startPage, int numberOfPages)
		{
			Debug.Assert(_writePage + numberOfPages <= _pager.NumberOfAllocatedPages);

			var result = _pager.Get(null, _writePage);

			for (var i = 0; i < numberOfPages; i++)
			{
				_pageTranslationTable[startPage + i] = _writePage;
				_writePage++;
			}

			return result;
		}

		public void Dispose()
		{
			_pager.Dispose();
		}
	}
}