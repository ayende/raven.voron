// -----------------------------------------------------------------------
//  <copyright file="UnmanagedBitsTests.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using Voron.Impl.FreeSpace;
using Xunit;

namespace Voron.Tests.Util
{
	public class UnmanagedBitsTests
	{
		[Fact]
		public void ShouldCalculateMaxNumberOfPagesAndModificationBitsAccordingToAllocated_OnePageSpace()
		{
			const int pageSize = 4096;

			var bytes = new byte[4096]; // allocate one page

			unsafe
			{
				fixed (byte* ptr = bytes)
				{
					var bits = new UnmanagedBits(ptr, 0, bytes.Length, 320, pageSize);

					Assert.Equal(320, bits.NumberOfTrackedPages);
					Assert.Equal(32728, bits.MaxNumberOfPages);
					Assert.Equal(1, bits.ModificationBits);
					Assert.Equal(1, bits.ModificationBytes);
					Assert.Equal(32736, bits.MaxNumberOfPages + (bits.ModificationBytes * 8));
				}
			}
		}

		[Fact]
		public void ShouldCalculateMaxNumberOfPagesAndModificationBitsAccordingToAllocated_TwoPageSpace()
		{
			const int pageSize = 4096;

			var bytes = new byte[2 * 4096]; // allocate two pages

			unsafe
			{
				fixed (byte* ptr = bytes)
				{
					var bits = new UnmanagedBits(ptr, 0, bytes.Length, 320, pageSize);

					Assert.Equal(320, bits.NumberOfTrackedPages);
					Assert.Equal(65496, bits.MaxNumberOfPages);
					Assert.Equal(2, bits.ModificationBits);
					Assert.Equal(1, bits.ModificationBytes);
					Assert.Equal(65504, bits.MaxNumberOfPages + (bits.ModificationBytes * 8));
				}
			}
		}

		[Fact]
		public void ShouldCalculateMaxNumberOfPagesAndModificationBitsAccordingToAllocated_TenPageSpace()
		{
			const int pageSize = 4096;

			var bytes = new byte[10 * 4096]; // allocate ten pages

			unsafe
			{
				fixed (byte* ptr = bytes)
				{
					var bits = new UnmanagedBits(ptr, 0, bytes.Length, 320, pageSize);

					Assert.Equal(320, bits.NumberOfTrackedPages);
					Assert.Equal(327632, bits.MaxNumberOfPages);
					Assert.Equal(10, bits.ModificationBits);
					Assert.Equal(2, bits.ModificationBytes);
					Assert.Equal(327648, bits.MaxNumberOfPages + (bits.ModificationBytes * 8));
				}
			}
		}

		[Fact]
		public void CanSetAndGetTheSameValues()
		{
			var bytes = new byte[UnmanagedBits.CalculateSizeInBytesForAllocation(128, 4096)];

			unsafe
			{
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
		}
	}
}