using System;
using System.Diagnostics;

namespace Voron.Impl.FreeSpace
{
	public unsafe class UnmanagedBits
	{
		private readonly byte* _rawPtr;
		private readonly long _sizeInBytes;
		private readonly long _capacity;
		private readonly long _modificationBits;
		private readonly long _maxNumberOfPages;
		private readonly long _numberOfPages;
		private readonly int _pageSize;
		private readonly int* _freePagesPtr;
		private readonly int* _dirtyFreePagesPtr;
		private readonly long _startPageNumber;

		public UnmanagedBits(byte* ptr, long startPageNumber, long sizeInBytes, long numberOfPages, int pageSize)
		{
			_rawPtr = ptr;
			_freePagesPtr = (int*)_rawPtr + sizeof(int);

			var sizeForBits = (sizeInBytes - sizeof (int)); // minus dirty flag

			_capacity = NumberOfBitsForAllocatedSizeInBytes(sizeForBits);

			_modificationBits = (sizeForBits + pageSize - 1) / pageSize;
			_maxNumberOfPages = _capacity - (ModificationBytes * 8);

			Debug.Assert(_modificationBits >= 1);
			Debug.Assert(_maxNumberOfPages >= 1);

			_dirtyFreePagesPtr = _freePagesPtr + _maxNumberOfPages /sizeof(int);

			_numberOfPages = numberOfPages;
			_pageSize = pageSize;

			_startPageNumber = startPageNumber;

			NativeMethods.memset(_rawPtr, 0, (int)_sizeInBytes); // clean all bits
		}

		public long NumberOfTrackedPages
		{
			get { return _numberOfPages; }
		}

		public long MaxNumberOfPages
		{
			get { return _maxNumberOfPages; }
		}

		public long StartPageNumber
		{
			get { return _startPageNumber; }
		}

		public long ModificationBits
		{
			get { return _modificationBits; }
		}

		public long ModificationBytes
		{
			get { return (_modificationBits + 8 - 1) / 8; }
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
			for (int i = 0; i < ModificationBits; i++)
			{
				SetBit(_dirtyFreePagesPtr, i, false); // mark clean
			}
		}

		private void SetBit(int* ptr, long pos, bool value)
		{
			if (pos < 0 || pos >= _capacity)
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

		public static long CalculateSizeInBytesForAllocation(long numberOfPages, int pageSize)
		{
			return GetSizeInBytesFor(
				sizeof(int) + // dirty bit - but need it aligned
				numberOfPages + // pages
				Math.Min(1, numberOfPages / pageSize)); // modified pages
		}

		public static long NumberOfBitsForAllocatedSizeInBytes(long allocatedBytes)
		{
			return allocatedBytes * 8;
		}

		private static long GetSizeInBytesFor(long numberOfBits)
		{
			return sizeof(int) * ((numberOfBits - 1) / 32 + 1);
		}

		public void CopyAllTo(UnmanagedBits other)
		{
			Debug.Assert(NumberOfTrackedPages == other.NumberOfTrackedPages && _sizeInBytes == other._sizeInBytes);

			NativeMethods.memcpy(other._rawPtr, _rawPtr, (int)_sizeInBytes);
		}

		public void CopyDirtyPagesTo(UnmanagedBits other)
		{
			for (int i = 0; i < ModificationBits; i++)
			{
				if (GetBit(_dirtyFreePagesPtr, ModificationBits, i) == false)
					continue;

				NativeMethods.memcpy((byte*)other._freePagesPtr + (_pageSize * i), (byte*)_freePagesPtr + (_pageSize * i), _pageSize);
			}
		}

		public bool IsFree(long pos)
		{
			return GetBit(_freePagesPtr, _numberOfPages, pos);
		}

		public void MoveTo(UnmanagedBits other)
		{
			// move dirty bit and all free pages
			NativeMethods.memmove(other._rawPtr, _rawPtr, (int)(GetSizeInBytesFor(NumberOfTrackedPages) + sizeof(int)));

			// move all modified pages
			NativeMethods.memmove((byte*)other._dirtyFreePagesPtr, (byte*)_dirtyFreePagesPtr,
								  (int)(GetSizeInBytesFor(ModificationBits)));
		}
	}
}