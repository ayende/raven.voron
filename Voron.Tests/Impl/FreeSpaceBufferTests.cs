// -----------------------------------------------------------------------
//  <copyright file="BitBufferTests.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using Voron.Impl;
using Voron.Impl.FreeSpace;
using Xunit;

namespace Voron.Tests.Impl
{
	// TODO arek
	//public unsafe class FreeSpaceBufferTests
	//{
	//	private int[] AllocateMemoryForBitBuffer(long numberOfPages)
	//	{
	//		return new int[UnmanagedBits.GetSizeOfIntArrayFor(1 + 2*numberOfPages)];
	//	}

	//	[Fact]
	//	public void FreeSpaceBufferConstructor()
	//	{
	//		var bytes = AllocateMemoryForBitBuffer(10);

	//		fixed (int* ptr = bytes)
	//		{
	//			var buffer = new FreeSpaceBuffer(ptr, 10);

	//			buffer.FreePages[0] = true;

	//			Assert.True(buffer.AllBits[1]);
	//		}
	//	}

	//	[Fact]
	//	public void ShouldReturnEmptyRangeOfFreeBits()
	//	{
	//		var bytes = AllocateMemoryForBitBuffer(10);

	//		fixed (int* ptr = bytes)
	//		{
	//			var buffer = new FreeSpaceBuffer(ptr, 10);

	//			var freePages = buffer.GetContinuousRangeOfFreePages(1);

	//			// should went through all of the pages but didn't find anything
	//			Assert.Null(freePages);
	//			Assert.Equal(10, buffer.lastSearchPosition);
	//		}
	//	}

	//	[Fact]
	//	public void ShouldReturnNextRangesOfFreeBits()
	//	{
	//		var bytes = AllocateMemoryForBitBuffer(10);

	//		fixed (int* ptr = bytes)
	//		{
	//			var buffer = new FreeSpaceBuffer(ptr, 10);

	//			var bits = buffer.FreePages;

	//			bits[0] = true;
	//			bits[1] = true;
	//			bits[2] = false;
	//			bits[3] = false;
	//			bits[4] = true;
	//			bits[5] = true;
	//			bits[6] = true;
	//			bits[7] = false;
	//			bits[8] = true;
	//			bits[9] = true;

	//			var freePages = buffer.GetContinuousRangeOfFreePages(2);

	//			Assert.Equal(freePages[0], 0);
	//			Assert.Equal(freePages[1], 1);

	//			freePages = buffer.GetContinuousRangeOfFreePages(3);

	//			Assert.Equal(freePages[0], 4);
	//			Assert.Equal(freePages[1], 5);
	//			Assert.Equal(freePages[2], 6);

	//			freePages = buffer.GetContinuousRangeOfFreePages(2);

	//			Assert.Equal(freePages[0], 8);
	//			Assert.Equal(freePages[1], 9);
	//		}
	//	}

	//	[Fact]
	//	public void ShouldWorkInCycle()
	//	{
	//		var bytes = AllocateMemoryForBitBuffer(6);

	//		fixed (int* ptr = bytes)
	//		{
	//			var buffer = new FreeSpaceBuffer(ptr, 6);

	//			var bits = buffer.FreePages;

	//			bits[0] = false;
	//			bits[1] = false;
	//			bits[2] = true;
	//			bits[3] = true;
	//			bits[4] = true;
	//			bits[5] = true;

	//			var freePages = buffer.GetContinuousRangeOfFreePages(2);

	//			Assert.Equal(freePages[0], 2);
	//			Assert.Equal(freePages[1], 3);
	//			Assert.Equal(4, buffer.lastSearchPosition);

	//			// free pages from the beginning
	//			bits[0] = true;
	//			bits[1] = true;

	//			freePages = buffer.GetContinuousRangeOfFreePages(2);

	//			Assert.Equal(freePages[0], 4);
	//			Assert.Equal(freePages[1], 5);
	//			Assert.Equal(6, buffer.lastSearchPosition);

	//			freePages = buffer.GetContinuousRangeOfFreePages(2);

	//			Assert.Equal(freePages[0], 0);
	//			Assert.Equal(freePages[1], 1);
	//		}
	//	}

	//	[Fact]
	//	public void InitiallyBufferIsNotDirty()
	//	{
	//		var bytes = AllocateMemoryForBitBuffer(1);

	//		fixed (int* ptr = bytes)
	//		{
	//			var buffer = new FreeSpaceBuffer(ptr, 1);
	//			Assert.False(buffer.IsDirty);
	//		}
	//	}

	//	[Fact]
	//	public void CanMarkBufferAsDirty()
	//	{
	//		var bytes = AllocateMemoryForBitBuffer(1);

	//		fixed (int* ptr = bytes)
	//		{
	//			var buffer = new FreeSpaceBuffer(ptr, 1);

	//			buffer.IsDirty = true;

	//			Assert.True(buffer.IsDirty);
	//		}
	//	}
	//}
}