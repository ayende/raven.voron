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
		private TransactionHeader* _currentTxHeader = null;

		public LogFile(IVirtualPager pager, long logNumber)
		{
			Number = logNumber;
			_pager = pager;
			_writePage = 0;
		}

		public long Number { get; private set; }

		public IEnumerable<long> Pages
		{
			get { return _pageTranslationTable.Keys; }
		}

		public void TransactionBegin(Transaction tx)
		{
			_currentTxHeader = GetTransactionHeader();

			_currentTxHeader->TxId = tx.Id;
			_currentTxHeader->NextPageNumber = tx.NextPageNumber;
			_currentTxHeader->LastPageNumber = -1;
			_currentTxHeader->PageCount = -1;
			_currentTxHeader->Crc = 0;
			_currentTxHeader->Marker = TransactionMarker.Start;

			_allocatedPagesInTransaction = 0;
		}

		public void TransactionSplit(Transaction tx)
		{
			if (_currentTxHeader != null)
			{
				_currentTxHeader->Marker |= TransactionMarker.Split;
				_currentTxHeader->PageCount = _allocatedPagesInTransaction;
			}
			else
			{
				_currentTxHeader = GetTransactionHeader();
				_currentTxHeader->TxId = tx.Id;
				_currentTxHeader->NextPageNumber = tx.NextPageNumber;
				_currentTxHeader->Marker = TransactionMarker.Split;
				_currentTxHeader->PageCount = -1;
				_currentTxHeader->Crc = 0;
			}	
		}

		public void TransactionCommit(Transaction tx)
		{
			LastCommittedTransactionId = tx.Id;
			LastPageNumberOfCommittedTransaction = tx.NextPageNumber - 1;

			_currentTxHeader->LastPageNumber = LastPageNumberOfCommittedTransaction;
			_currentTxHeader->Crc = 0; //TODO
			_currentTxHeader->Marker |= TransactionMarker.End;
			_currentTxHeader->PageCount = _allocatedPagesInTransaction;
			tx.Environment.Root.State.CopyTo(&_currentTxHeader->Root);
		}

		public long LastCommittedTransactionId { get; private set; }

		public long LastPageNumberOfCommittedTransaction { get; private set; }

		private TransactionHeader* GetTransactionHeader()
		{
			if (_lastFlushedPage != _writePage - 1)
				_writePage = _lastFlushedPage + 1;

			var result = (TransactionHeader*) Allocate(-1, 1).Base;

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

			if (startPage != -1) // internal use - transaction header allocation
			{
				// we allocate more than one page only if the page is an overflow
				// so here we don't want to create mapping for them too
				_pageTranslationTable[startPage] = _writePage;

				_allocatedPagesInTransaction++; // TODO not sure if here should't we add all overflow pages
			}

			_writePage += numberOfPages;

			return result;
		}

		public void Dispose()
		{
			_pager.Dispose();
		}
	}
}