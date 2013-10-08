// -----------------------------------------------------------------------
//  <copyright file="LogFile.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Diagnostics;
using Voron.Trees;

namespace Voron.Impl.Log
{
	public unsafe class LogFile : IDisposable
	{
		private readonly IVirtualPager _pager;
		private readonly Dictionary<long, long> _pageTranslationTable = new Dictionary<long, long>(); // TODO after restart we need to recover those values
		private long _writePage;//TODO after restart we need to recover this value
		private long _lastFlushedPage = -1;
		private int _allocatedPagesInTransaction = 0;
		private LogEntryHeader* _currentEntryHeader = null;

		public LogFile(IVirtualPager pager)
		{
			_pager = pager;
			_writePage = 0;
		}

		public IVirtualPager Pager
		{
			get { return _pager; }
		}

		public void TransactionBegin(Transaction tx)
		{
			var header = CreateHeader(tx);
			header->TransactionMarker = TransactionStateMarker.Start;
			
			_currentEntryHeader = header;
			_allocatedPagesInTransaction = 0;
		}

		public void TransactionSplit(Transaction tx)
		{
			if (_currentEntryHeader != null)
			{
				_currentEntryHeader->TransactionMarker |= TransactionStateMarker.Split;
				_currentEntryHeader->PageCount = _allocatedPagesInTransaction - 1; // minus header page
			}
			else
			{
				var header = CreateHeader(tx);
				header->TransactionMarker = TransactionStateMarker.Split;
				_currentEntryHeader = header;
			}
		}

		public void TransactionCommit()
		{
			_currentEntryHeader->TransactionMarker |= TransactionStateMarker.Commit;
			_currentEntryHeader->PageCount = _allocatedPagesInTransaction - 1; // minus header page
		}

		private LogEntryHeader* CreateHeader(Transaction tx)
		{
			if (_lastFlushedPage != _writePage - 1)
				_writePage = _lastFlushedPage + 1;

			return (LogEntryHeader*) Allocate(tx, _writePage, 1).Base;
		}

		public long AvailablePages
		{
			get { return _pager.NumberOfAllocatedPages - _writePage - 1; }
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

		public Page Allocate(Transaction tx, long startPage, int numberOfPages)
		{
			Debug.Assert(_writePage + numberOfPages <= _pager.NumberOfAllocatedPages);

			var result = _pager.Get(tx, _writePage);

			for (var i = 0; i < numberOfPages; i++)
			{
				_pageTranslationTable[startPage + i] = _writePage;
				_writePage++;
			}

			_allocatedPagesInTransaction += numberOfPages;

			return result;
		}

		public void Dispose()
		{
			_pager.Dispose();
		}
	}
}