// -----------------------------------------------------------------------
//  <copyright file="UnmanagedBitsTests.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Linq;
using Voron.Impl.FreeSpace;
using Xunit;

namespace Voron.Tests.Impl.Free
{
	public unsafe class UnmanagedBitsTests
	{
		[Fact]
		public void InitiallyAllPagesShouldBeMarkedAsBusy()
		{
			var bytes = new byte[4096];

			fixed (byte* ptr = bytes)
			{
				var bits = new UnmanagedBits(ptr, 0, bytes.Length, 10000, 4096);
				for (int i = 0; i < bits.NumberOfTrackedPages; i++)
				{
					Assert.False(bits.IsFree(i));
				}
			}
		}

		[Fact]
		public void ShouldCalculateMaxNumberOfPagesAndModificationBitsAccordingToAllocated_OnePageSpace()
		{
			const int pageSize = 4096;

			var bytes = new byte[4096]; // allocate one page
			fixed (byte* ptr = bytes)
			{
				var bits = new UnmanagedBits(ptr, 0, bytes.Length, 320, pageSize);

				Assert.Equal(320, bits.NumberOfTrackedPages);
				Assert.Equal(32704, bits.MaxNumberOfPages);
				Assert.Equal(1, bits.AllModificationBits);
				Assert.Equal(1, bits.ModificationBitsInUse);
				Assert.Equal(4, bits.BytesTakenByModificationBits);
				Assert.Equal(32736, bits.MaxNumberOfPages + (bits.BytesTakenByModificationBits * 8));
			}
		}

		[Fact]
		public void ShouldCalculateMaxNumberOfPagesAndModificationBitsAccordingToAllocated_TwoPageSpace()
		{
			const int pageSize = 4096;

			var bytes = new byte[2 * 4096]; // allocate two pages
			fixed (byte* ptr = bytes)
			{
				var bits = new UnmanagedBits(ptr, 0, bytes.Length, 40000, pageSize);

				Assert.Equal(40000, bits.NumberOfTrackedPages);
				Assert.Equal(65472, bits.MaxNumberOfPages);
				Assert.Equal(2, bits.AllModificationBits);
				Assert.Equal(2, bits.ModificationBitsInUse);
				Assert.Equal(4, bits.BytesTakenByModificationBits);
				Assert.Equal(65504, bits.MaxNumberOfPages + (bits.BytesTakenByModificationBits * 8));
			}
		}

		[Fact]
		public void ShouldCalculateMaxNumberOfPagesAndModificationBitsAccordingToAllocated_TenPageSpace()
		{
			const int pageSize = 4096;

			var bytes = new byte[10 * 4096]; // allocate ten pages

			fixed (byte* ptr = bytes)
			{
				var bits = new UnmanagedBits(ptr, 0, bytes.Length, 90000, pageSize);

				Assert.Equal(90000, bits.NumberOfTrackedPages);
				Assert.Equal(327616, bits.MaxNumberOfPages);
				Assert.Equal(10, bits.AllModificationBits);
				Assert.Equal(3, bits.ModificationBitsInUse);
				Assert.Equal(4, bits.BytesTakenByModificationBits);
				Assert.Equal(327648, bits.MaxNumberOfPages + (bits.BytesTakenByModificationBits * 8));
			}
		}

		[Fact]
		public void CanSetAndGetTheSameValues()
		{
			var bytes = new byte[UnmanagedBits.CalculateSizeInBytesForAllocation(128, 4096)];

			fixed (byte* ptr = bytes)
			{
				var bits = new UnmanagedBits(ptr, 0, bytes.Length, 128, 4096);

				var modeFactor = new Random().Next(1, 7);

				for (int i = 0; i < 128; i++)
				{
					bits.MarkPage(i, (i % modeFactor) == 0);
				}

				for (int i = 0; i < 128; i++)
				{
					Assert.Equal(bits.IsFree(i), (i % modeFactor) == 0);
				}
			}
		}

		[Fact]
		public void WhenCopyDirtyPagesShouldLimitCopiedBytesAccordingToUsedFreePages_OnePage()
		{
			const int pageSize = 4096;

			var bytes1 = new byte[4096];
			var bytes2 = new byte[4096];

			fixed (byte* ptr1 = bytes1)
			fixed (byte* ptr2 = bytes2)
			{
				var bits1 = new UnmanagedBits(ptr1, 0, bytes1.Length, 20, pageSize);
				var bits2 = new UnmanagedBits(ptr2, 0, bytes2.Length, 20, pageSize);

				bits1.MarkPage(10, true);

				var bytesCopied = bits1.CopyDirtyBitsTo(bits2);

				Assert.Equal(3, bytesCopied);
			}
		}

		[Fact]
		public void WhenCopyDirtyPagesShouldLimitCopiedBytesAccordingToUsedFreePages_TwoPages()
		{
			const int pageSize = 4096;

			var bytes1 = new byte[2 * 4096];
			var bytes2 = new byte[2 * 4096];

			fixed (byte* ptr1 = bytes1)
			{
				fixed (byte* ptr2 = bytes2)
				{
					var bits1 = new UnmanagedBits(ptr1, 0, bytes1.Length, 60000, pageSize);
					var bits2 = new UnmanagedBits(ptr2, 0, bytes2.Length, 60000, pageSize);

					bits1.MarkPage(10, true);
					bits1.MarkPage(40000, true);

					var bytesCopied = bits1.CopyDirtyBitsTo(bits2);

					Assert.Equal(4096 + 3404, bytesCopied);
				}
			}
		}

		[Fact]
		public void CanCopyBufferToBufferWithTheSameSize()
		{
			const int pageSize = 4096;

			var bytes1 = new byte[2 * pageSize];
			var bytes2 = new byte[2 * pageSize];

			fixed (byte* ptr1 = bytes1)
			fixed (byte* ptr2 = bytes2)
			{
				// 65472 - is the max number of bits for buffer with size 8192 bytes
				var bits1 = new UnmanagedBits(ptr1, 0, bytes1.Length, 65472, pageSize);
				var bits2 = new UnmanagedBits(ptr2, 0, bytes2.Length, 65472, pageSize);

				var rand = new Random();
				var maxNumberOfChanges = rand.Next(20000, 40000);

				for (int i = 0; i < maxNumberOfChanges; i++)
				{
					var bitToSet = rand.Next(0, 65471);
					bits1.MarkPage(bitToSet, true);
				}

				bits1.CopyAllTo(bits2);

				for (var i = 0; i < 2*pageSize; i++)
				{
					Assert.Equal(ptr1[i], ptr2[i]);
				}
			}
		}

		[Fact]
		public void CanCopyBufferToBiggerBuffer()
		{
			const int pageSize = 4096;

			var bytes1 = new byte[2 * pageSize];
			var bytes2 = new byte[3 * pageSize];

			fixed (byte* ptr1 = bytes1)
			fixed (byte* ptr2 = bytes2)
			{
				// 65472 - is the max number of bits for buffer with size 8192 bytes
				var bits1 = new UnmanagedBits(ptr1, 0, bytes1.Length, 65472, pageSize);

				// 98240 - is the max number of bits for buffer with size 12288 bytes
				var bits2 = new UnmanagedBits(ptr2, 0, bytes2.Length, 98240, pageSize);

				var rand = new Random();
				var maxNumberOfChanges = rand.Next(20000, 40000);

				for (int i = 0; i < maxNumberOfChanges; i++)
				{
					var bitToSet = rand.Next(0, 65471);
					bits1.MarkPage(bitToSet, true);
				}

				bits1.CopyAllTo(bits2);

				for (var i = 0; i < 65472 / 8; i++)
				{
					Assert.Equal(ptr1[i], ptr2[i]);
				}
			}
		}

		[Fact]
		public void CanSetRangeOfBits()
		{
			var bytes = new byte[2 *4096];

			fixed (byte* ptr = bytes)
			{
				var bits = new UnmanagedBits(ptr, 0, bytes.Length, 60000, 4096);

				var freePages = Enumerable.Range(30000, 20000).ToArray();

				bits.MarkPages(30000, 20000, true);

				for (var page = 0; page < 60000; page++)
				{
					if (freePages.Contains(page))
					{
						Assert.True(bits.IsFree(page));
					}
					else
					{
						Assert.False(bits.IsFree(page));
					}
				}
			}
		}

		[Fact]
		public void MarkingRangeOfBitsShouldSetAppropriateModificationBits()
		{
			var bytes = new byte[2 * 4096];

			fixed (byte* ptr = bytes)
			{
				var bits = new UnmanagedBits(ptr, 0, bytes.Length, 60000, 4096);

				const long lastBitInFirstBufferPage = 32768 - 1;

				bits.MarkPages(lastBitInFirstBufferPage, 2, true); // should mark modification bits as true on positionss 0 and 1

				var modificationBits = bits.GetModificationBits();

				Assert.True(modificationBits[0]);
				Assert.True(modificationBits[1]);
			}
		}
	}
}