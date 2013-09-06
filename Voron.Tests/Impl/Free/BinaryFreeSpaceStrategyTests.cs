// -----------------------------------------------------------------------
//  <copyright file="BinaryFreeSpaceStrategyTests.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using Voron.Impl;
using Voron.Impl.FileHeaders;
using Voron.Impl.FreeSpace;
using Xunit;

namespace Voron.Tests.Impl.Free
{
	public unsafe class BinaryFreeSpaceStrategyTests : IDisposable
	{
		private readonly IVirtualPager pager;
		private readonly BinaryFreeSpaceStrategy freeSpace;
		private const long NumberOfTrackedPages = 10;

		public BinaryFreeSpaceStrategyTests()
		{
			pager = new PureMemoryPager(new byte[2 * 4096]);

			// create pages for free space buffer
			pager.Get(null, 0);
			pager.Get(null, 1);

			freeSpace = new BinaryFreeSpaceStrategy(n => new IntPtr(pager.AcquirePagePointer(n)));

			var header = new FreeSpaceHeader
				{
					FirstBufferPageNumber = 0,
					SecondBufferPageNumber = 1,
					NumberOfPagesTakenForTracking = 1,
					NumberOfTrackedPages = NumberOfTrackedPages,
					PageSize = pager.PageSize
				};

			freeSpace.Initialize(&header);
		}

		[Fact]
		public void ConsecutiveTransactionsShouldUseBuffersAlternately()
		{
			freeSpace.SetBufferForTransaction(0);
			var buff0 = freeSpace.CurrentBuffer;
			freeSpace.OnCommit();

			freeSpace.SetBufferForTransaction(1);
			var buff1 = freeSpace.CurrentBuffer;
			freeSpace.OnCommit();

			freeSpace.SetBufferForTransaction(2);
			var buff2 = freeSpace.CurrentBuffer;
			freeSpace.OnCommit();

			freeSpace.SetBufferForTransaction(3);
			var buff3 = freeSpace.CurrentBuffer;
			freeSpace.OnCommit();

			Assert.Same(buff0, buff2);
			Assert.Same(buff1, buff3);
			Assert.NotSame(buff0, buff1);
		}

		[Fact]
		public void ShouldNotFindAnyFreePagesWhenThereAreNone()
		{
			freeSpace.SetBufferForTransaction(0);
			var freePageStart = freeSpace.Find(1);

			Assert.Equal(-1, freePageStart);
		}

		[Fact]
		public void CanGetFreePagesFromTheBegin()
		{
			const int tx = 0;
			freeSpace.SetBufferForTransaction(tx);

			freeSpace.RegisterFreePages(new List<long> {0, 1});
			freeSpace.OnCommit(); // will mark registered pages as free in the buffer

			var firstFreePage = freeSpace.Find(2);

			Assert.Equal(0, firstFreePage);
		}

		[Fact]
		public void CanGetFreePagesFromTheEnd()
		{
			const int tx = 0;
			freeSpace.SetBufferForTransaction(tx);

			freeSpace.RegisterFreePages(new List<long> { NumberOfTrackedPages - 1,  NumberOfTrackedPages - 2 });
			freeSpace.OnCommit();

			var firstFreePage = freeSpace.Find(2);

			Assert.Equal(NumberOfTrackedPages - 2, firstFreePage);
		}

		[Fact]
		public void CanGetFreePagesFromTheMiddle()
		{
			const int tx = 0;
			freeSpace.SetBufferForTransaction(tx);

			freeSpace.RegisterFreePages(new List<long> { 5, 6 });
			freeSpace.OnCommit();

			var firstFreePage = freeSpace.Find(2);

			Assert.Equal(5, firstFreePage);
		}

		[Fact]
		public void GettingFreePagesShouldMarkThemAsBusy()
		{
			const int tx = 0;
			freeSpace.SetBufferForTransaction(tx);

			freeSpace.RegisterFreePages(new List<long> { 4, 5, 6 });
			freeSpace.OnCommit();

			var firstFreePage = freeSpace.Find(2);

			Assert.False(freeSpace.CurrentBuffer.IsFree(4));
			Assert.False(freeSpace.CurrentBuffer.IsFree(5));
			Assert.True(freeSpace.CurrentBuffer.IsFree(6));
		}

		[Fact]
		public void WillGetFirstRangeWithEnoughFreePages()
		{
			const int tx = 0;
			freeSpace.SetBufferForTransaction(tx);

			freeSpace.RegisterFreePages(new List<long> {1, 4, 5, 7, 8, 9});
			freeSpace.OnCommit();

			var firstFreePage = freeSpace.Find(2);
			Assert.Equal(4, firstFreePage);

			firstFreePage = freeSpace.Find(1);
			Assert.Equal(7, firstFreePage);

			firstFreePage = freeSpace.Find(2);
			Assert.Equal(8, firstFreePage);
		}

		[Fact]
		public void MustNotReturnLessFreePagesThanRequested()
		{
			const int tx = 0;
			freeSpace.SetBufferForTransaction(tx);

			freeSpace.RegisterFreePages(new List<long> { 2, 3 });
			freeSpace.OnCommit();

			// 2 pages are free but we request for 3
			var firstFreePage = freeSpace.Find(3);
			Assert.Equal(-1, firstFreePage);
		}

		[Fact]
		public void MustNotMergeFreePagesFromEndAndBegin()
		{
			const int tx = 0;
			freeSpace.SetBufferForTransaction(tx);

			freeSpace.RegisterFreePages(new List<long> { 0, 6, 7, 8, 9 });
			freeSpace.OnCommit();

			var firstFreePage = freeSpace.Find(2);
			Assert.Equal(6, firstFreePage);

			firstFreePage = freeSpace.Find(3);
			Assert.Equal(-1, firstFreePage);
		}

		[Fact]
		public void WhenBufferIsDirtyShouldCopySecondBufferBeforeCanProcess()
		{
			const int tx1 = 0;
			const int tx2 = 1;
			freeSpace.SetBufferForTransaction(tx1);

			freeSpace.RegisterFreePages(new List<long> {1, 3});
			freeSpace.OnCommit();

			freeSpace.SetBufferForTransaction(tx2);

			freeSpace.RegisterFreePages(new List<long> {4, 7, 9});
			// freeSpace.OnCommit(); - do not commit, so buffer will get marked as dirty

			freeSpace.SetBufferForTransaction(tx2); // should copy dirty pages from a buffer for tx1

			// the same free pages as in buffer for tx1
			Assert.True(freeSpace.CurrentBuffer.IsFree(1));
			Assert.True(freeSpace.CurrentBuffer.IsFree(3));

			Assert.False(freeSpace.CurrentBuffer.IsFree(4));
			Assert.False(freeSpace.CurrentBuffer.IsFree(7));
			Assert.False(freeSpace.CurrentBuffer.IsFree(9));
		}

		[Fact]
		public void SearchingFreePagesShouldWorkInCycle()
		{
			const int tx1 = 0;
			freeSpace.SetBufferForTransaction(tx1);

			freeSpace.RegisterFreePages(new List<long> {6, 7, 8, 9});
			freeSpace.OnCommit();

			var firstFreePage = freeSpace.Find(2);
			Assert.Equal(6, firstFreePage);

			// free pages from the beginning
			freeSpace.RegisterFreePages(new List<long> {0, 1});
			freeSpace.OnCommit();

			// move search pointer at the end
			firstFreePage = freeSpace.Find(2);
			Assert.Equal(8, firstFreePage);

			// should take from the begin
			firstFreePage = freeSpace.Find(2);
			Assert.Equal(0, firstFreePage);
		}

		public void Dispose()
		{
			pager.Dispose();
		}
	}
}