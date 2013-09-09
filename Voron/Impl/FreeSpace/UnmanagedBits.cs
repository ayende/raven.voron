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
		private const int DirtyFlag = sizeof(int);

		private readonly long sizeInBytes;
		private readonly long capacity;
		private readonly int pageSize;

		private byte* rawPtr;
		private int* freePagesPtr;
		private int* modificationBitsPtr;

		public UnmanagedBits(byte* ptr, long startPageNumber, long sizeInBytes, long numberOfPages, int pageSize)
		{
			this.sizeInBytes = sizeInBytes;
			this.pageSize = pageSize;

			NumberOfTrackedPages = numberOfPages;
			StartPageNumber = startPageNumber;

			var sizeForBits = sizeInBytes - DirtyFlag;

			capacity = NumberOfBitsForSizeInBytes(sizeForBits);

			AllModificationBits = DivideAndRoundUp(sizeForBits, pageSize);
			MaxNumberOfPages = capacity - NumberOfBitsForSizeInBytes(BytesTakenByModificationBits);
			ModificationBitsInUse = (long)Math.Ceiling((NumberOfTrackedPages / 8f) / this.pageSize);

			Debug.Assert(AllModificationBits >= 1);
			Debug.Assert(MaxNumberOfPages >= 1);
			Debug.Assert(numberOfPages <= MaxNumberOfPages);

			SetBufferPointer(ptr);

			NativeMethods.memset(rawPtr, 0, (int)this.sizeInBytes); // clean all bits

			TotalNumberOfFreePages = 0;
		}

		public long NumberOfTrackedPages { get; private set; }

		public long MaxNumberOfPages { get; private set; }

		public long StartPageNumber { get; private set; }

		public long ModificationBitsInUse { get; private set; }

		public long AllModificationBits { get; private set; }

		public long BytesTakenByModificationBits
		{
			get { return sizeof(int) * DivideAndRoundUp(AllModificationBits, 32); }
		}

		internal long TotalNumberOfFreePages { get; set; }

		public bool IsDirty
		{
			get { return *(rawPtr) != 0; }
			set { *(rawPtr) = (byte)(value ? 1 : 0); }
		}

		public void MarkPage(long page, bool val)
		{
			SetBit(freePagesPtr, page, val);
			SetBit(modificationBitsPtr, page/(pageSize*8), true); // mark dirty
		}

		public void ResetModifiedPages()
		{
			for (int i = 0; i < AllModificationBits; i++)
			{
				SetBit(modificationBitsPtr, i, false); // mark clean
			}
		}

		private void SetBit(int* ptr, long pos, bool value)
		{
			if (pos < 0 || pos >= capacity)
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
			var sizeForFreePages = GetSizeInBytesFor(numberOfPages);
			var numberOfModificationBits = DivideAndRoundUp(sizeForFreePages, pageSize);
			var sizeForModificationBits = sizeof(int) * DivideAndRoundUp(numberOfModificationBits, 32);

			return DirtyFlag + sizeForFreePages + sizeForModificationBits;
		}

		public static long NumberOfBitsForSizeInBytes(long allocatedBytes)
		{
			return allocatedBytes * 8;
		}

		private static long GetSizeInBytesFor(long numberOfBits)
		{
			return sizeof(int) * ((numberOfBits - 1) / 32 + 1);
		}

		public void CopyAllTo(UnmanagedBits other)
		{
			if (sizeInBytes == other.sizeInBytes)
			{
				Debug.Assert(NumberOfTrackedPages == other.NumberOfTrackedPages && AllModificationBits == other.AllModificationBits);

				NativeMethods.memcpy(other.rawPtr, rawPtr, (int) sizeInBytes);
			}
			else
			{
				if (sizeInBytes > other.sizeInBytes)
					throw new InvalidOperationException(
						string.Format(
							"Cannot copy unmanaged bits because the size of a buffer is not enough. The size of buffer that is being copied is {0} while the size of buffer where it should be copied is {1}",
							sizeInBytes, other.sizeInBytes));

				// here we need to split the data that we copy because the size of buffers is different what means
				// that the pointers of the modification bits have different positions relative to the beginning of a buffer

				// copy dirty bit and all free pages
				NativeMethods.memcpy(other.rawPtr, rawPtr, (int)(GetSizeInBytesFor(NumberOfTrackedPages) + DirtyFlag));

				// copy all modification bits
				NativeMethods.memcpy((byte*)other.modificationBitsPtr, (byte*)modificationBitsPtr,
									  (int)BytesTakenByModificationBits);
			}

			other.RefreshNumberOfFreePages();
		}

		public long CopyDirtyPagesTo(UnmanagedBits other)
		{
			var copied = 0;

			for (var i = 0; i < ModificationBitsInUse; i++)
			{
				if (GetBit(modificationBitsPtr, ModificationBitsInUse, i) == false)
					continue;

				var toCopy = pageSize;

				if (i == ModificationBitsInUse - 1) // last piece of free bits can take less bytes than pageSize
					toCopy = (int)DivideAndRoundUp(NumberOfTrackedPages - (pageSize * i * 8), 8);

				NativeMethods.memcpy((byte*)other.freePagesPtr + (pageSize * i), (byte*)freePagesPtr + (pageSize * i), toCopy);

				copied += toCopy;
			}

			if(copied > 0)
				other.RefreshNumberOfFreePages();

			return copied;
		}

		public bool IsFree(long pos)
		{
			return GetBit(freePagesPtr, NumberOfTrackedPages, pos);
		}

		private static long DivideAndRoundUp(long numerator, long denominator)
		{
			return (numerator + denominator - 1) / denominator;
		}

		public void IncreaseSize(long newNumberOfPagesToTrack)
		{
			if (MaxNumberOfPages < newNumberOfPagesToTrack)
				throw new InvalidOperationException(
					string.Format(
						"Cannot increase the size of unmanaged bits buffer to {0}, because it can contains only {1} number of bits",
						newNumberOfPagesToTrack, MaxNumberOfPages));

			NumberOfTrackedPages = newNumberOfPagesToTrack;

			ModificationBitsInUse = (long)Math.Ceiling((NumberOfTrackedPages / 8f) / pageSize);
		}

		public void SetBufferPointer(byte* ptr)
		{
			rawPtr = ptr;
			freePagesPtr = (int*)(rawPtr + DirtyFlag);

			var numberOfIntValuesReservedForFreeBits = MaxNumberOfPages / 32;

			modificationBitsPtr = freePagesPtr + numberOfIntValuesReservedForFreeBits;
		}

		public void RefreshNumberOfFreePages()
		{
			TotalNumberOfFreePages = 0L;

			for (int i = 0; i < NumberOfTrackedPages; i++)
			{
				if (IsFree(i))
				{
					TotalNumberOfFreePages++;
				}
			}
		}
	}
}