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
		private TransactionHeader* _currentEntryHeader = null;

		public LogFile(IVirtualPager pager)
		{
			_pager = pager;
			_writePage = 0;
		}

		public IVirtualPager Pager
		{
			get { return _pager; }
		}

		public IEnumerable<long> Pages
		{
			get { return _pageTranslationTable.Keys; }
		}

		public void TransactionBegin(Transaction tx)
		{
			_currentEntryHeader = TransactionHeader();

			_currentEntryHeader->TxId = tx.Id;
			_currentEntryHeader->NextPageNumber = tx.NextPageNumber;
			_currentEntryHeader->PageCount = -1;
			_currentEntryHeader->Crc = 0;
			_currentEntryHeader->Marker = TransactionMarker.Start;
			
			_allocatedPagesInTransaction = 0;
		}

		public void TransactionSplit(Transaction tx)
		{
			if (_currentEntryHeader != null)
			{
				_currentEntryHeader->Marker |= TransactionMarker.Split;
				_currentEntryHeader->PageCount = _allocatedPagesInTransaction;
			}
			else
			{
				_currentEntryHeader = TransactionHeader();
				_currentEntryHeader->Marker = TransactionMarker.Split;
				_currentEntryHeader->PageCount = -1;
				_currentEntryHeader->Crc = 0;
			}

			_currentEntryHeader->TxId = tx.Id;
			_currentEntryHeader->NextPageNumber = tx.NextPageNumber;
	
		}

		public void TransactionCommit(Transaction tx)
		{
			_currentEntryHeader->TxId = tx.Id;
			_currentEntryHeader->NextPageNumber = tx.NextPageNumber;

			_currentEntryHeader->Crc = 0; //TODO

			_currentEntryHeader->Marker |= TransactionMarker.End;
			_currentEntryHeader->PageCount = _allocatedPagesInTransaction;
		}

		private TransactionHeader* TransactionHeader()
		{
			if (_lastFlushedPage != _writePage - 1)
				_writePage = _lastFlushedPage + 1;

			var requestedPage = _writePage;
			var result = (TransactionHeader*) Allocate(_writePage, 1).Base;

			// header page should not be counted as page allocated in transaction
			_allocatedPagesInTransaction--;
			_pageTranslationTable.Remove(requestedPage);

			return result;
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

		public Page ReadPage(Transaction tx, long pageNumber)
		{
			if (_pageTranslationTable.ContainsKey(pageNumber) == false)
				return null;

			return _pager.Read(tx, _pageTranslationTable[pageNumber]);
		}

		public Page Allocate(long startPage, int numberOfPages)
		{
			Debug.Assert(_writePage + numberOfPages <= _pager.NumberOfAllocatedPages);

			var result = _pager.GetWritable(_writePage);

			// we allocate more than one page only if the page is an overflow
			// so here we don't want to create mapping for them too
			_pageTranslationTable[startPage] = _writePage;
			
			// but we need to move the index
			_writePage += numberOfPages;

			_allocatedPagesInTransaction++;

			return result;
		}

		public void Dispose()
		{
			_pager.Dispose();
		}
	}
}