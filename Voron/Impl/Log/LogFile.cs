// -----------------------------------------------------------------------
//  <copyright file="LogFile.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using Voron.Trees;

namespace Voron.Impl.Log
{
	public unsafe class LogFile : IDisposable
	{
		private readonly IVirtualPager _pager;
		private readonly Dictionary<long, long> _pageTranslationTable = new Dictionary<long, long>();
		private long _writePage = 0;
		private long _lastSyncedPage = -1;
		private int _allocatedPagesInTransaction = 0;
		private TransactionHeader* _currentTxHeader = null;

		public LogFile(IVirtualPager pager, long logNumber)
		{
			Number = logNumber;
			_pager = pager;
			_writePage = 0;
		}

		public LogFile(IVirtualPager pager, long logNumber, long lastSyncedPage)
			: this(pager, logNumber)
		{
			_lastSyncedPage = lastSyncedPage;
			_writePage = lastSyncedPage + 1;
		}


		public long Number { get; private set; }

		public long LastSyncedPage
		{
			get { return _lastSyncedPage; }
		}

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
			_currentTxHeader->TxMarker = TransactionMarker.Start;

			_allocatedPagesInTransaction = 0;
		}

		public void TransactionSplit(Transaction tx)
		{
			if (_currentTxHeader != null)
			{
				_currentTxHeader->TxMarker |= TransactionMarker.Split;
				_currentTxHeader->PageCount = _allocatedPagesInTransaction;
			}
			else
			{
				_currentTxHeader = GetTransactionHeader();
				_currentTxHeader->TxId = tx.Id;
				_currentTxHeader->NextPageNumber = tx.NextPageNumber;
				_currentTxHeader->TxMarker = TransactionMarker.Split;
				_currentTxHeader->PageCount = -1;
				_currentTxHeader->Crc = 0;
			}	
		}

		public void TransactionCommit(Transaction tx)
		{
			LastCommittedTransactionId = tx.Id;
			LastPageNumberOfLastCommittedTransaction = tx.NextPageNumber - 1;

			_currentTxHeader->LastPageNumber = LastPageNumberOfLastCommittedTransaction;
			_currentTxHeader->Crc = 0; //TODO
			_currentTxHeader->TxMarker |= TransactionMarker.End;
			_currentTxHeader->PageCount = _allocatedPagesInTransaction;
			tx.Environment.Root.State.CopyTo(&_currentTxHeader->Root);
			//TODO free space copy
		}

		public long LastCommittedTransactionId { get; private set; }

		public long LastPageNumberOfLastCommittedTransaction { get; private set; }

		private TransactionHeader* GetTransactionHeader()
		{
			if (_lastSyncedPage != _writePage - 1)
				_writePage = _lastSyncedPage + 1;

			var result = (TransactionHeader*) Allocate(-1, 1).Base;
			result->HeaderMarker = Constants.TransactionHeaderMarker;

			return result;
		}

		public long AvailablePages
		{
			get { return _pager.NumberOfAllocatedPages - _writePage - 1; }
		}

		public void Sync()
		{
			var start = _lastSyncedPage + 1;
			var count = _writePage - start;

			_pager.Flush(start, count);
			_pager.Sync();

			_lastSyncedPage += count;
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

		public TransactionHeader* RecoverAndValidate(long startReadingPage, TransactionHeader* previous)
		{
			TransactionHeader* lastReadHeader = previous;

			var readPosition = startReadingPage;

			while (readPosition < _pager.NumberOfAllocatedPages)
			{
				var current = (TransactionHeader*)_pager.Read(null, readPosition).Base;

				if(current->HeaderMarker != Constants.TransactionHeaderMarker)
					break;

				ValidateHeader(current, lastReadHeader);

				if (current->TxMarker.HasFlag(TransactionMarker.End) == false && current->TxMarker.HasFlag(TransactionMarker.Split) == false)
				{
					readPosition += current->PageCount + current->OverflowPageCount;
					continue;
				}

				lastReadHeader = current;

				readPosition++;

				for (var i = 0; i < current->PageCount; i++)
				{
					var page = _pager.Read(null, readPosition);

					_pageTranslationTable[page.PageNumber] = readPosition;

					if (page.IsOverflow)
						readPosition += Page.GetNumberOfOverflowPages(_pager.PageSize, page.OverflowSize);
					else
						readPosition++;

					_lastSyncedPage = readPosition - 1;
					_writePage = _lastSyncedPage + 1;
				}
			}

			return lastReadHeader;
		}

		private void ValidateHeader(TransactionHeader* current, TransactionHeader* previous)
		{
			if (current->TxId < 0)
				throw new InvalidDataException("Transaction id cannot be less than 0 (Tx: " + current->TxId);
			if (current->TxMarker.HasFlag(TransactionMarker.Start) == false)
				throw new InvalidDataException("Transaction must have Start marker");
			if (current->LastPageNumber < 0)
				throw new InvalidDataException("Last page number after committed transaction must be greater than 0");

			if (previous == null) 
				return;

			if (previous->TxMarker.HasFlag(TransactionMarker.Split))
			{
				if(current->TxMarker.HasFlag(TransactionMarker.Split) == false)
					throw new InvalidDataException("Previous transaction have a split marker, so the current one should have it too");

				if (current->TxId == previous->TxId)
					throw new InvalidDataException("Split transaction should have the same id in the log. Expected id: " +
					                               previous->TxId + ", got: " + current->TxId);
			}
			else
			{
				if (current->TxId != 1 && // 1 is a first storage transaction which does not increment transaction counter after commit
					current->TxId - previous->TxId != 1)
					throw new InvalidDataException("Unexpected transaction id. Expected: " + (previous->TxId + 1) + ", got:" +
					                               current->TxId);
			}
			
		}
	}
}