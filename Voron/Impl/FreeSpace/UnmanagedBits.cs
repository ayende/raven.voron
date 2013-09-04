using System;
using System.Diagnostics;

namespace Voron.Impl.FreeSpace
{
	/// <summary>
	/// Here we keep data in the following format
	/// [dirty_flag][free_pages_bits][modification_bits]
	/// dirty_flag - an integer value (4 bytes) indicating if buffer is dirty or nor
	/// free_pages_bits - an array of bits that reflect if page is free (true) or not (false)
	/// modification_bits - any modification in a continuous range of 32784 free_pages_bits (which is 4096 [pageSize] bytes of data) is reflected as one bit
	///
	/// Notes: 
	/// - we always allocate twice a much memory as we actually need to track numberOfPages. That's why we calculate maximum numbers of pages that we can track here
	/// </summary>
	public unsafe class UnmanagedBits
	{
		private readonly byte* _rawPtr;
		private readonly long _sizeInBytes;
		private readonly long _capacity;
		private readonly long _allModificationBits;
		private readonly long _maxNumberOfPages;
		private readonly long _numberOfPages;
		private readonly int _pageSize;
		private readonly int* _freePagesPtr;
		private readonly int* _modificationBitsPtr;
		private readonly long _startPageNumber;
		private long _modificationBitsInUse;

		public UnmanagedBits(byte* ptr, long startPageNumber, long sizeInBytes, long numberOfPages, int pageSize)
		{
			_rawPtr = ptr;
			_freePagesPtr = (int*)_rawPtr + sizeof(int);

			var sizeForBits = (sizeInBytes - sizeof (int)); // minus dirty flag

			_capacity = NumberOfBitsForAllocatedSizeInBytes(sizeForBits);

			_allModificationBits = DivideAndRoundUp(sizeForBits, pageSize);
			_maxNumberOfPages = _capacity - NumberOfBitsForAllocatedSizeInBytes(BytesTakenByModificationBits);

			Debug.Assert(_allModificationBits >= 1);
			Debug.Assert(_maxNumberOfPages >= 1);
			Debug.Assert(numberOfPages <= _maxNumberOfPages);

			_modificationBitsPtr = _freePagesPtr + _maxNumberOfPages/(sizeof (int)*8);

			_numberOfPages = numberOfPages;
			_pageSize = pageSize;

			_modificationBitsInUse = (long)Math.Ceiling((_numberOfPages / 8f) / _pageSize);

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

		public long ModificationBitsInUse
		{
			get { return _modificationBitsInUse; }
		}

		public long AllModificationBits
		{
			get { return _allModificationBits; }
		}

		public long BytesTakenByModificationBits
		{
			get { return sizeof(int) * DivideAndRoundUp(_allModificationBits, 32); }
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
			SetBit(_modificationBitsPtr, page / (_pageSize * 8), true); // mark dirty
		}

		public void ResetModifiedPages()
		{
			for (int i = 0; i < AllModificationBits; i++)
			{
				SetBit(_modificationBitsPtr, i, false); // mark clean
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
			var dirtyFlag = sizeof (int);
			var sizeForFreePages = GetSizeInBytesFor(numberOfPages);
			var numberOfModificationBits = DivideAndRoundUp(sizeForFreePages, pageSize);
			var sizeForModificationBits = sizeof (int)*DivideAndRoundUp(numberOfModificationBits, 32);

			return dirtyFlag + sizeForFreePages + sizeForModificationBits;
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

		public long CopyDirtyPagesTo(UnmanagedBits other)
		{
			var copied = 0;

			for (int i = 0; i < ModificationBitsInUse; i++)
			{
				if (GetBit(_modificationBitsPtr, ModificationBitsInUse, i) == false)
					continue;

				int toCopy = _pageSize;

				if (i == ModificationBitsInUse - 1) // last piece of free bits can take less bytes than pageSize
					toCopy = (int) DivideAndRoundUp(_numberOfPages - (_pageSize*i*8), 8);

				NativeMethods.memcpy((byte*)other._freePagesPtr + (_pageSize * i), (byte*)_freePagesPtr + (_pageSize * i), toCopy);

				copied += toCopy;
			}

			return copied;
		}

		public bool IsFree(long pos)
		{
			return GetBit(_freePagesPtr, _numberOfPages, pos);
		}

		public void MoveTo(UnmanagedBits other)
		{
			// move dirty bit and all free pages
			NativeMethods.memmove(other._rawPtr, _rawPtr, (int) (GetSizeInBytesFor(NumberOfTrackedPages) + sizeof (int)));

			// move all modified pages
			NativeMethods.memmove((byte*) other._modificationBitsPtr, (byte*) _modificationBitsPtr,
			                      (int) BytesTakenByModificationBits);
		}

		private static long DivideAndRoundUp(long numerator, long denominator)
		{
			return (numerator + denominator - 1) / denominator;
		}
	}
}