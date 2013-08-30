using System.Diagnostics;
using System.IO;

namespace Voron.Impl.FreeSpace
{
	public class BinaryFreeSpaceStrategy
	{
		readonly UnmanagedBits[] bits = new UnmanagedBits[2];

		private UnmanagedBits _current;
		private long _lastSearchPosition;

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
				Debug.Assert(next.Size == reference.Size);
				next.IsDirty = true;

				reference.CopyDirtyPagesTo(next);
			}
			
			next.ResetModifiedPages();

			_current = next;
		}

		public long Find(int numberOfFreePages)
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

		private long GetContinuousRangeOfFreePages(int numberOfPagesToGet)
		{
			Debug.Assert(numberOfPagesToGet > 0);

			var searched = 0;

			long rangeStart = -1;
			var rangeSize = 0;
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
	}
}