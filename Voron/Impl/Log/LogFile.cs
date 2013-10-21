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
using Voron.Util;

namespace Voron.Impl.Log
{
	public class TransactionInLog
	{
		public long Id;
		public long LastPageNumber;
	}

	public unsafe class LogFile : IDisposable
	{
		private const int pagesTakenByHeader = 1;

		private readonly IVirtualPager _pager;
		private readonly Dictionary<long, long> _pageTranslationTable = new Dictionary<long, long>();
		private readonly Dictionary<long, long> _transactionPageTranslationTable = new Dictionary<long, long>();
		private long _writePage = 0;
		private long _lastSyncedPage = -1;
		private int _allocatedPagesInTransaction = 0;
		private int _overflowPagesInTransaction = 0;
		private TransactionHeader* _currentTxHeader = null;

		public LogFile(IVirtualPager pager, long logNumber)
		{
			Number = logNumber;
			_pager = pager;
			_writePage = 0;

			LastCommittedTransaction = new TransactionInLog();
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

		internal long WritePagePosition
		{
			get { return _writePage; }
		}

		public IEnumerable<long> ModifiedPageNumbers
		{
			get { return _pageTranslationTable.Keys; }
		}

		public TransactionInLog LastCommittedTransaction { get; set; }

		public bool LastTransactionCommitted
		{
			get
			{
				if (_currentTxHeader != null)
				{
					Debug.Assert(_currentTxHeader->TxMarker.HasFlag(TransactionMarker.Commit) == false);
					return false;
				}
				return true;
			}
		}

		public void TransactionBegin(Transaction tx)
		{
			if (LastTransactionCommitted == false)
			{
				// last transaction did not commit, we need to move back the write page position
				_writePage = _lastSyncedPage + 1;
			}

			_currentTxHeader = GetTransactionHeader();

			_currentTxHeader->TxId = tx.Id;
			_currentTxHeader->NextPageNumber = tx.NextPageNumber;
			_currentTxHeader->LastPageNumber = -1;
			_currentTxHeader->PageCount = -1;
			_currentTxHeader->Crc = 0;
			_currentTxHeader->TxMarker = TransactionMarker.Start;

			_allocatedPagesInTransaction = 0;
			_overflowPagesInTransaction = 0;

			_transactionPageTranslationTable.Clear();
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
			foreach (var translation in _transactionPageTranslationTable)
			{
				_pageTranslationTable[translation.Key] = translation.Value;
			}

			LastCommittedTransaction.Id = tx.Id;
			LastCommittedTransaction.LastPageNumber = tx.NextPageNumber - 1;

			_currentTxHeader->LastPageNumber = tx.NextPageNumber - 1;
			_currentTxHeader->TxMarker |= TransactionMarker.Commit;
			_currentTxHeader->PageCount = _allocatedPagesInTransaction;
			_currentTxHeader->OverflowPageCount = _overflowPagesInTransaction;
			tx.Environment.Root.State.CopyTo(&_currentTxHeader->Root);

			var crcOffset = (int) (_currentTxHeader->PageNumberInLogFile + pagesTakenByHeader)*_pager.PageSize;
			var crcCount = (_allocatedPagesInTransaction + _overflowPagesInTransaction)*_pager.PageSize;

			_currentTxHeader->Crc = Crc.Value(_pager.PagerState.Base, crcOffset, crcCount);

			//TODO free space copy

			_currentTxHeader = null;

			Sync();
		}


		private TransactionHeader* GetTransactionHeader()
		{
			var result = (TransactionHeader*) Allocate(-1, pagesTakenByHeader).Base;
			result->HeaderMarker = Constants.TransactionHeaderMarker;
			result->PageNumberInLogFile = _writePage - pagesTakenByHeader;

			return result;
		}

		public long AvailablePages
		{
			get { return _pager.NumberOfAllocatedPages - _writePage; }
		}

		private void Sync()
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
			{
				if (_currentTxHeader != null && _currentTxHeader->TxId == tx.Id && _transactionPageTranslationTable.ContainsKey(pageNumber))
					return _pager.Read(_transactionPageTranslationTable[pageNumber]);
				return null;
			}

			return _pager.Read(_pageTranslationTable[pageNumber]);
		}

		public Page Allocate(long startPage, int numberOfPages)
		{
			Debug.Assert(_writePage + numberOfPages <= _pager.NumberOfAllocatedPages);

			var result = _pager.GetWritable(_writePage);

			if (startPage != -1) // internal use - transaction header allocation, so we don't want to count it as allocated by transaction
			{
				// we allocate more than one page only if the page is an overflow
				// so here we don't want to create mapping for them too
				_transactionPageTranslationTable[startPage] = _writePage;

				_allocatedPagesInTransaction++;

				if (numberOfPages > 1)
				{
					_overflowPagesInTransaction += (numberOfPages - 1);
				}
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
				var current = (TransactionHeader*)_pager.Read(readPosition).Base;

				if(current->HeaderMarker != Constants.TransactionHeaderMarker)
					break;

				ValidateHeader(current, lastReadHeader);

				if (current->TxMarker.HasFlag(TransactionMarker.Commit) == false && current->TxMarker.HasFlag(TransactionMarker.Split) == false)
				{
					readPosition += current->PageCount + current->OverflowPageCount;
					continue;
				}

				lastReadHeader = current;

				readPosition++;

				var transactionPageTranslation = new Dictionary<long, long>();

				uint crc = 0;

				for (var i = 0; i < current->PageCount; i++)
				{
					var page = _pager.Read(readPosition);

					transactionPageTranslation[page.PageNumber] = readPosition;

					if (page.IsOverflow)
					{
						var numOfPages = Page.GetNumberOfOverflowPages(_pager.PageSize, page.OverflowSize);
						readPosition += numOfPages;
						crc = Crc.Extend(crc, page.Base, 0, numOfPages*_pager.PageSize);
					}
					else
					{
						readPosition++;
						crc = Crc.Extend(crc, page.Base, 0, _pager.PageSize);
					}

					_lastSyncedPage = readPosition - 1;
					_writePage = _lastSyncedPage + 1;
				}

				if (crc != current->Crc)
				{
					throw new InvalidDataException("Checksum mismatch"); //TODO this is temporary, ini the future this condition will just mean that transaction was not committed
				}

				foreach (var translation in transactionPageTranslation)
				{
					_pageTranslationTable[translation.Key] = translation.Value;
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
			if(current->PageCount > 0 && current->Crc == 0)
				throw new InvalidDataException("Transaction checksum can't be equal to 0");

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