using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using Voron.Impl.FileHeaders;
using Voron.Trees;

namespace Voron.Impl.FreeSpace
{
	public class BinaryFreeSpaceStrategy
	{
		public class FreeSpaceInfo
		{
			private readonly BinaryFreeSpaceStrategy _strategy;

			public FreeSpaceInfo(BinaryFreeSpaceStrategy strategy)
			{
				_strategy = strategy;
			}

		    public long FreePagesCount
		    {
		        get
		        {
		            var bits = _strategy.bits.First(x => x.IsDirty == false);
		            return bits.TotalNumberOfFreePages;
		        }
		    }

		    public List<long> GetBuffersPages()
			{
				var range = new List<long>();

				var buffer1 = _strategy.bits[0];
				var buffer2 = _strategy.bits[1];

				for (var i = buffer1.StartPageNumber; i < buffer1.StartPageNumber + _strategy.state.NumberOfPagesTakenForTracking; i++)
				{
					range.Add(i);
				}

				for (var i = buffer2.StartPageNumber; i < buffer2.StartPageNumber + _strategy.state.NumberOfPagesTakenForTracking; i++)
				{
					range.Add(i);
				}

				return range;
			}

			public List<long> GetFreePages(long transactionNumber)
			{
			    var buffer = _strategy.bits[transactionNumber & 1]; // take buffer specific for transaction

			    var result = new List<long>();

			    for (var i = 0; i < buffer.NumberOfTrackedPages; i++)
			    {
			        if (buffer.IsFree(i))
			            result.Add(i);
			    }

			    return result;
			}
		}

		public class FreedTransaction
		{
			public long Id;
			public List<long> Pages;
		}

		private readonly Func<long, IntPtr> acquirePagePointer;
		private readonly UnmanagedBits[] bits = new UnmanagedBits[2];
		private readonly LinkedList<FreedTransaction> registeredFreedPages = new LinkedList<FreedTransaction>();

		private FreeSpaceHeader state;
		private long _lastSearchPosition = -1;
		private bool initialized;
		private FreeSpaceInfo _info;

		public long MaxNumberOfPages
		{
			get
			{
				Debug.Assert((bits[0] == null && bits[1] == null) || (bits[0].MaxNumberOfPages == bits[1].MaxNumberOfPages));

				return bits[0] != null ? bits[0].MaxNumberOfPages : 0;
			}
		}

		public decimal NumberOfTrackedPages
		{
			get
			{
				Debug.Assert((bits[0] == null && bits[1] == null) || (bits[0].NumberOfTrackedPages == bits[1].NumberOfTrackedPages));

				return bits[0] != null ? bits[0].NumberOfTrackedPages : 0;
			}
		}

		public FreeSpaceInfo Info
		{
			get { return _info ?? (_info = new FreeSpaceInfo(this)); }
		}

		public BinaryFreeSpaceStrategy(Func<long, IntPtr> acquirePagePointer)
		{
			this.acquirePagePointer = acquirePagePointer;
		}

		public unsafe void Initialize(FreeSpaceHeader* header)
		{
			bits[0] = new UnmanagedBits((byte*) acquirePagePointer(header->FirstBufferPageNumber).ToPointer(),
			                            header->FirstBufferPageNumber, header->NumberOfPagesTakenForTracking*header->PageSize,
			                            header->NumberOfTrackedPages, header->PageSize);
			bits[0].RefreshNumberOfFreePages();

			bits[1] = new UnmanagedBits((byte*) acquirePagePointer(header->SecondBufferPageNumber).ToPointer(),
			                            header->SecondBufferPageNumber, header->NumberOfPagesTakenForTracking*header->PageSize,
			                            header->NumberOfTrackedPages, header->PageSize);
			bits[1].RefreshNumberOfFreePages();

			state = new FreeSpaceHeader
				{
					FirstBufferPageNumber = header->FirstBufferPageNumber,
					SecondBufferPageNumber = header->SecondBufferPageNumber,
					NumberOfPagesTakenForTracking = header->NumberOfPagesTakenForTracking,
					NumberOfTrackedPages = header->NumberOfTrackedPages,
					PageSize = header->PageSize
				};

			initialized = true;
		}

		public unsafe void UpdateBufferPointers()
		{
			bits[0].SetBufferPointer((byte*)acquirePagePointer(state.FirstBufferPageNumber).ToPointer());
			bits[1].SetBufferPointer((byte*)acquirePagePointer(state.SecondBufferPageNumber).ToPointer());
		}

		public Page TryAllocateFromFreeSpace(Transaction tx, int numberOfPages)
		{
			if (initialized == false)
				return null; // this can happen the first time free space is initialized

			if (bits[tx.Id & 1] == null)
				throw new InvalidOperationException("The current buffer for transaction is null. Did you forget to set buffer for transaction?");

			var page = Find(tx, numberOfPages);

			if (page == -1)
				return null;

			var newPage = tx.Pager.Get(tx, page);
			newPage.PageNumber = page;
			return newPage;
		}

		public void SetBufferForTransaction(Transaction tx)
		{
			var indexOfBuffer = tx.Id & 1;

			var next = bits[indexOfBuffer];
			var reference = bits[1 - indexOfBuffer];

			if (next.IsDirty)
			{
				if (reference.IsDirty)
					throw new InvalidDataException(
						"Both buffers are dirty. Valid state of the free pages buffer cannot be restored. Transaction number: " +
						tx.Id); // should never happen

				reference.CopyAllTo(next);
			}
			else // copy dirty bits from reference
			{
				reference.CopyDirtyBitsTo(next);
			}
			
			next.InitializeProcessing();
		}

		public long Find(Transaction tx, long numberOfFreePages)
		{
			var result = GetContinuousRangeOfFreePages(tx, numberOfFreePages);

			if (result != -1)
			{
				var buffer = bits[tx.Id & 1];

				for (var i = result; i < result + numberOfFreePages; i++)
				{
					buffer.MarkPage(i, false);// mark returned pages as busy
				}

				buffer.TotalNumberOfFreePages -= numberOfFreePages;
			}

			return result;
		}

		private long GetContinuousRangeOfFreePages(Transaction tx, long numberOfPagesToGet)
		{
			Debug.Assert(numberOfPagesToGet > 0);

			if (initialized == false)
				return -1;

			var buffer = bits[tx.Id & 1];

			if (numberOfPagesToGet > buffer.TotalNumberOfFreePages)
				return -1;

			var searched = 0;

			long rangeStart = -1;
			long rangeSize = 0;

			var end = buffer.NumberOfTrackedPages - 1;

			while (searched < buffer.NumberOfTrackedPages)
			{
				searched++;

				if (_lastSearchPosition == end)
				{
					_lastSearchPosition = -1;
					rangeStart = -1;
					rangeSize = 0;
				}

				_lastSearchPosition++;

				if (buffer.IsFree(_lastSearchPosition))
				{
					if (rangeSize == 0L) // nothing found
					{
						rangeStart = _lastSearchPosition;
					}
					rangeSize++;
					if (rangeSize == numberOfPagesToGet)
						break;
					continue; // check next page
				}

				rangeSize = 0;
				rangeStart = -1;
			}

			if (rangeSize < numberOfPagesToGet)
				return -1;

			Debug.Assert(rangeSize == numberOfPagesToGet);

			return rangeStart;
		}

		public unsafe void MoveTo(long firstBufferPageStart, long secondBufferPageStart, long numberOfPagesToTrack, long numberOfPagesForTracking, int pageSize)
		{
			var newFirstBuffer = new UnmanagedBits((byte*) acquirePagePointer(firstBufferPageStart),
			                                       firstBufferPageStart,
			                                       numberOfPagesForTracking*pageSize,
			                                       numberOfPagesToTrack,
			                                       pageSize);

			bits[0].CopyAllTo(newFirstBuffer); // we cannot just copy because a new buffer is bigger
			bits[0] = newFirstBuffer;

			var newSecondBuffer = new UnmanagedBits((byte*) acquirePagePointer(secondBufferPageStart),
			                                        secondBufferPageStart,
			                                        numberOfPagesForTracking*pageSize,
			                                        numberOfPagesToTrack,
			                                        pageSize);
			bits[1].CopyAllTo(newSecondBuffer);
			bits[1] = newSecondBuffer;

			// mark pages that was taken by old buffers as free
			var oldHeader = state;

			bits[0].MarkPages(oldHeader.FirstBufferPageNumber, oldHeader.NumberOfPagesTakenForTracking, true);
			bits[1].MarkPages(oldHeader.FirstBufferPageNumber, oldHeader.NumberOfPagesTakenForTracking, true);

			bits[0].MarkPages(oldHeader.SecondBufferPageNumber, oldHeader.NumberOfPagesTakenForTracking, true);
			bits[1].MarkPages(oldHeader.SecondBufferPageNumber, oldHeader.NumberOfPagesTakenForTracking, true);


			// update header
			state = new FreeSpaceHeader
			{
				FirstBufferPageNumber = firstBufferPageStart,
				SecondBufferPageNumber = secondBufferPageStart,
				NumberOfTrackedPages = numberOfPagesToTrack,
				NumberOfPagesTakenForTracking = numberOfPagesForTracking,
				PageSize = pageSize
			};
		}

		public void RegisterFreePages(Transaction tx, List<long> freedPages)
		{
			registeredFreedPages.AddLast(new FreedTransaction()
				{
					Id = tx.Id,
					Pages = freedPages
				});
		}

		public void OnCommit(Transaction tx, long oldestTx, out List<long> dirtyPages)
		{
			var buffer = bits[tx.Id & 1];

			while (registeredFreedPages.First != null && registeredFreedPages.First.Value.Id <= oldestTx)
			{
				var val = registeredFreedPages.First.Value;
				registeredFreedPages.RemoveFirst();

				foreach (var freedPage in val.Pages)
				{
					buffer.MarkPage(freedPage, true);
				}

				buffer.TotalNumberOfFreePages += val.Pages.Count;
			}

			dirtyPages = buffer.DirtyPages;

			buffer.Processed();
		}

		public unsafe void CopyStateTo(FreeSpaceHeader* freeSpaceHeader)
		{
			freeSpaceHeader->FirstBufferPageNumber = state.FirstBufferPageNumber;
			freeSpaceHeader->SecondBufferPageNumber = state.SecondBufferPageNumber;
			freeSpaceHeader->NumberOfTrackedPages = state.NumberOfTrackedPages;
			freeSpaceHeader->NumberOfPagesTakenForTracking = state.NumberOfPagesTakenForTracking;
			freeSpaceHeader->PageSize = state.PageSize;
		}

		public void TrackMorePages(long newNumberOfPagesToTrack)
		{
			foreach (var unmanagedBits in bits)
			{
				unmanagedBits.IncreaseSize(newNumberOfPagesToTrack);
			}
		}

		public bool IsFree(Transaction tx, long pageNumber)
		{
			return bits[tx.Id & 1].IsFree(pageNumber);
		}
	}
}