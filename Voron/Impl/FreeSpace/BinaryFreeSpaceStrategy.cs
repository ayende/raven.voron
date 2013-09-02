using System;
using System.Diagnostics;
using System.IO;
using Voron.Impl.FileHeaders;

namespace Voron.Impl.FreeSpace
{
	public class BinaryFreeSpaceStrategy
	{
		readonly UnmanagedBits[] bits = new UnmanagedBits[2];

		private UnmanagedBits _current;
		private long _lastSearchPosition;

		public decimal MaxNumberOfPages
		{
			get { return _current != null ? _current.MaxNumberOfPages : 0; }
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
				next.IsDirty = true;
			}
			else // copy dirty pages from reference
			{
				next.IsDirty = true;

				reference.CopyDirtyPagesTo(next);
			}
			
			next.ResetModifiedPages();

			_current = next;
		}

		public long Find(long numberOfFreePages)
		{
			var result = GetContinuousRangeOfFreePages(numberOfFreePages);

			if (result != -1)
			{
				for (long i = result; i < numberOfFreePages; i++)
				{
					_current.MarkPage(i, false);// mark returned pages as busy
				}
			}

			return result;
		}

		private long GetContinuousRangeOfFreePages(long numberOfPagesToGet)
		{
			Debug.Assert(numberOfPagesToGet > 0);

			var searched = 0;

			long rangeStart = -1;
			long rangeSize = 0;
			while (searched < _current.NumberOfTrackedPages)
			{
				searched++;

				if (_lastSearchPosition >= _current.NumberOfTrackedPages)
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

			if (rangeSize == 0)
				return -1;

			Debug.Assert(rangeSize == numberOfPagesToGet);

			return rangeStart;
		}

		public unsafe void MoveTo(long firstBufferStartPage, long secondBufferStartPage, long currentNumberOfAllocatedPages,
		                          long actualSize, int pageSize, Func<long, IntPtr> acquirePagePointer)
		{
			// here we need to copy the old values to the new values
			// note that:
			// actual page size & actual size will be tracked in the file header, along with the page numbers
			// of the 1st & 2nd free space ranges

			if (bits[0] == null || bits[1] == null)
			{
				bits[0] = new UnmanagedBits((byte*) acquirePagePointer(firstBufferStartPage).ToPointer(), actualSize,
				                            currentNumberOfAllocatedPages, pageSize);
				bits[1] = new UnmanagedBits((byte*) acquirePagePointer(secondBufferStartPage).ToPointer(), actualSize,
				                            currentNumberOfAllocatedPages, pageSize);
			}
			else
			{
				var newFirstBuffer = new UnmanagedBits((byte*) acquirePagePointer(firstBufferStartPage), actualSize,
				                                       currentNumberOfAllocatedPages,
				                                       pageSize);
				bits[0].MoveTo(newFirstBuffer);
				bits[0] = newFirstBuffer;

				var newSecondBuffer = new UnmanagedBits((byte*) acquirePagePointer(secondBufferStartPage), actualSize,
				                                        currentNumberOfAllocatedPages,
				                                        pageSize);
				bits[1].MoveTo(newSecondBuffer);
				bits[1] = newSecondBuffer;

				//todo mark old pages as free
			}

			var header = new FreeSpaceHeader
				{
					BuffersSize = actualSize,
					FirstBufferPageNumber = firstBufferStartPage,
					SecondBufferPageNumber = secondBufferStartPage,
					PageSize = pageSize
				};
		}
	}
}