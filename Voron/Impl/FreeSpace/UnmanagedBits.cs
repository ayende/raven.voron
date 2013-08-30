using System;
using System.Diagnostics;

namespace Voron.Impl.FreeSpace
{
	public unsafe class UnmanagedBits
	{
		private readonly byte* _rawPtr;
		private readonly long _size;
		private readonly long _numberOfPages;
		private readonly int _pageSize;
		private readonly int* _freePagesPtr;
		private readonly int* _dirtyFreePagesPtr;

		public long NumberOfTrackedPages { get { return _numberOfPages; } }

		public UnmanagedBits(byte* ptr, long size, long numberOfPages, int pageSize)
		{
			_rawPtr = ptr;
			_freePagesPtr = (int*)_rawPtr + sizeof(int);
			_dirtyFreePagesPtr = _freePagesPtr + (numberOfPages / sizeof(int));
			_size = size;
			_numberOfPages = numberOfPages;
			_pageSize = pageSize;
		}

		public long Size
		{
			get { return _size; }
		}

		public bool IsDirty
		{
			get { return *(_rawPtr) != 0; }
			set { *(_rawPtr) = (byte)(value ? 1 : 0); }
		}

		public byte* RawPtr
		{
			get { return _rawPtr; }
		}

		public void MarkPage(long page, bool val)
		{
			SetBit(_freePagesPtr, page, val);
			SetBit(_dirtyFreePagesPtr, page / _pageSize, true); // mark dirty
		}


		public void ResetModifiedPages()
		{
			for (int i = 0; i < NumberOfTrackedPages; i++)
			{
				SetBit(_dirtyFreePagesPtr, i, false); // mark clean
			}
		}

		private void SetBit(int* ptr, long pos, bool value)
		{
			if (pos < 0 || pos >= _size)
				throw new ArgumentOutOfRangeException("pos");

			if (value)
				ptr[pos >> 5] |= (1 << (int)(pos & 31)); // '>> 5' is '/ 32', '& 31' is '% 32'
			else
				ptr[pos >> 5] &= ~(1 << (int)(pos & 31));
		}

		private bool GetBit(int* ptr, long size, long pos)
		{
			if (pos < 0 || pos >= size)
				throw new ArgumentOutOfRangeException("pos");

			return (ptr[pos >> 5] & (1 << (int)(pos & 31))) != 0;
		}

		public long CalculateSizeInBytesForAllocation(long numberOfPages)
		{
			return GetSizeInBytesFor(
				sizeof(int) + // dirty bit - but need it aligned
				numberOfPages + // pages
				Math.Min(1, numberOfPages / _pageSize)); // modified pages
		}

		public static long GetSizeOfIntArrayFor(long numberOfBits)
		{
			if (numberOfBits <= 0)
				return 0;

			return (numberOfBits - 1) / 32 + 1;
		}

		public static long GetSizeInBytesFor(long numberOfBits)
		{
			if (numberOfBits <= 0)
				return 0;

			return sizeof(int) * GetSizeOfIntArrayFor(numberOfBits);
		}

		public void CopyAllTo(UnmanagedBits other)
		{
			Debug.Assert(NumberOfTrackedPages == other.NumberOfTrackedPages && Size == other.Size);

			NativeMethods.memcpy(other._rawPtr, _rawPtr, (int)GetSizeInBytesFor(_size));
		}

		public void CopyDirtyPagesTo(UnmanagedBits other)
	    {
		    for (int i = 0; i < _numberOfPages; i++)
		    {
				if (GetBit(_dirtyFreePagesPtr, _numberOfPages / _pageSize, i) == false)
				    continue;

				NativeMethods.memcpy((byte*)other._freePagesPtr + (_pageSize * i), (byte*)_freePagesPtr + (_pageSize * i), _pageSize);
		    }
	    }

		public bool IsFree(long pos)
		{
			return GetBit(_freePagesPtr, _size, pos);
		}
	}
}