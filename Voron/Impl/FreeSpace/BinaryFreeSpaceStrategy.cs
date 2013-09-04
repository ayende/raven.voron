using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using Voron.Impl.FileHeaders;
using Voron.Trees;

namespace Voron.Impl.FreeSpace
{
	public class BinaryFreeSpaceStrategy
	{
		private readonly Func<long, IntPtr> acquirePagePointer;
		private readonly UnmanagedBits[] bits = new UnmanagedBits[2];
		private readonly List<long> registeredFreedPages = new List<long>();

		private FreeSpaceHeader state;
		private UnmanagedBits _current;
		private long _lastSearchPosition = -1;
		private bool initialized;

		public decimal MaxNumberOfPages
		{
			get
			{
				Debug.Assert((bits[0] == null && bits[1] == null) || (bits[0].MaxNumberOfPages == bits[1].MaxNumberOfPages));

				return bits[0] != null ? bits[0].MaxNumberOfPages : 0;
			}
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
			bits[1] = new UnmanagedBits((byte*) acquirePagePointer(header->SecondBufferPageNumber).ToPointer(),
			                            header->SecondBufferPageNumber, header->NumberOfPagesTakenForTracking*header->PageSize,
			                            header->NumberOfTrackedPages, header->PageSize);

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

		public Page TryAllocateFromFreeSpace(Transaction tx, int num)
		{
			if (initialized == false)
				return null; // this can happen the first time free space is initialized

			if(_current == null)
				throw new ArgumentNullException("_current", "The current buffer for transaction is null. Did you forget to set buffer for transaction?");

			var page = Find(num);

			if (page == -1)
				return null;

			var newPage = tx.Pager.Get(tx, page);
			newPage.PageNumber = page;
			return newPage;
		}

		public void SetBufferForTransaction(long transactionNumber)
		{
			var indexOfBuffer = transactionNumber & 1;

			var next = bits[indexOfBuffer];
			var reference = bits[1 - indexOfBuffer];

			if (next.IsDirty)
			{
				if (reference.IsDirty)
					throw new InvalidDataException(
						"Both buffers are dirty. Valid state of the free pages buffer cannot be restored. Transaction number: " +
						transactionNumber); // should never happen

				reference.CopyAllTo(next);
			}
			else // copy dirty pages from reference
			{
				reference.CopyDirtyPagesTo(next);
			}
			
			next.ResetModifiedPages();
			next.IsDirty = true;

			_current = next;
		}

		public long Find(long numberOfFreePages)
		{
			var result = GetContinuousRangeOfFreePages(numberOfFreePages);

			if (result != -1)
			{
				for (long i = result; i < result + numberOfFreePages; i++)
				{
					_current.MarkPage(i, false);// mark returned pages as busy
				}
			}

			return result;
		}

		private long GetContinuousRangeOfFreePages(long numberOfPagesToGet)
		{
			Debug.Assert(numberOfPagesToGet > 0);

			if (initialized == false)
				return -1;

			var searched = 0;

			long rangeStart = -1;
			long rangeSize = 0;
			while (searched < _current.NumberOfTrackedPages)
			{
				searched++;

				if (_lastSearchPosition >= _current.NumberOfTrackedPages - 1)
					_lastSearchPosition = -1;

				_lastSearchPosition++;

				if (_current.IsFree(_lastSearchPosition))
				{
					if (rangeSize == 0) // nothing found
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
			bits[0].MoveTo(newFirstBuffer);
			bits[0] = newFirstBuffer;

			var newSecondBuffer = new UnmanagedBits((byte*) acquirePagePointer(secondBufferPageStart),
			                                        secondBufferPageStart,
			                                        numberOfPagesForTracking*pageSize,
			                                        numberOfPagesToTrack,
			                                        pageSize);
			bits[1].MoveTo(newSecondBuffer);
			bits[1] = newSecondBuffer;

			// mark pages that was taken by old buffers as free
			var oldHeader = state;

			for (var i = oldHeader.FirstBufferPageNumber; i < oldHeader.FirstBufferPageNumber + oldHeader.NumberOfPagesTakenForTracking; i++)
			{
				bits[0].MarkPage(i, true);
				bits[1].MarkPage(i, true);
			}

			for (var i = oldHeader.SecondBufferPageNumber; i < oldHeader.SecondBufferPageNumber + oldHeader.NumberOfPagesTakenForTracking; i++)
			{
				bits[0].MarkPage(i, true);
				bits[1].MarkPage(i, true);
			}

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

		public void RegisterFreePages(List<long> freedPages)
		{
			registeredFreedPages.AddRange(freedPages);
		}

		public void OnCommit()
		{
			ReleasePages();

			_current.IsDirty = false;
		}

		private void ReleasePages()
		{
			foreach (var freedPage in registeredFreedPages)
			{
				_current.MarkPage(freedPage, true);
			}

			registeredFreedPages.Clear();
		}

		public List<long> GetBufferPages()
		{
			var range = new List<long>();

			for (var i = _current.StartPageNumber; i < _current.StartPageNumber + state.NumberOfPagesTakenForTracking; i++)
			{
				range.Add(i);
			}

			return range;
		}

		

		public unsafe void CopyStateTo(FreeSpaceHeader* freeSpaceHeader)
		{
			freeSpaceHeader->FirstBufferPageNumber = state.FirstBufferPageNumber;
			freeSpaceHeader->SecondBufferPageNumber = state.SecondBufferPageNumber;
			freeSpaceHeader->NumberOfTrackedPages = state.NumberOfTrackedPages;
			freeSpaceHeader->NumberOfPagesTakenForTracking = state.NumberOfPagesTakenForTracking;
			freeSpaceHeader->PageSize = state.PageSize;
		}
	}
}