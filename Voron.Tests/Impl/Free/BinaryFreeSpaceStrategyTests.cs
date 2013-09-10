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
	public class BinaryFreeSpaceStrategyTests : StorageTest
	{
		[Fact]
		public void ShouldNotFindAnyFreePagesWhenThereAreNone()
		{
			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				var freePageStart = Env.FreeSpaceHandling.Find(tx, 1);

				Assert.Equal(-1, freePageStart);
			}
		}

		[Fact]
		public void CanGetFreePagesFromTheBegin()
		{
			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				Env.FreeSpaceHandling.RegisterFreePages(tx, new List<long> { 0, 1 });
				List<long> _;
				Env.FreeSpaceHandling.OnCommit(tx, Env.OldestTransaction, out _); // will mark registered pages as free in the buffer

				var firstFreePage = Env.FreeSpaceHandling.Find(tx, 2);

				Assert.Equal(0, firstFreePage);
			}
		}

		[Fact]
		public void CanGetFreePagesFromTheEnd()
		{
			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				var numberOfTrackedPages = (long)Env.FreeSpaceHandling.NumberOfTrackedPages;

				Env.FreeSpaceHandling.RegisterFreePages(tx, new List<long> {numberOfTrackedPages - 1, numberOfTrackedPages - 2});
				List<long> _;
				Env.FreeSpaceHandling.OnCommit(tx, Env.OldestTransaction, out _);

				var firstFreePage = Env.FreeSpaceHandling.Find(tx, 2);

				Assert.Equal(numberOfTrackedPages - 2, firstFreePage);
			}
		}

		[Fact]
		public void CanGetFreePagesFromTheMiddle()
		{
			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				Env.FreeSpaceHandling.RegisterFreePages(tx, new List<long> {5, 6});
				List<long> _;
				Env.FreeSpaceHandling.OnCommit(tx, Env.OldestTransaction, out _);

				var firstFreePage = Env.FreeSpaceHandling.Find(tx, 2);

				Assert.Equal(5, firstFreePage);
			}
		}

		[Fact]
		public void GettingFreePagesShouldMarkThemAsBusy()
		{
			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				Env.FreeSpaceHandling.RegisterFreePages(tx, new List<long> {4, 5, 6});
				List<long> _;
				Env.FreeSpaceHandling.OnCommit(tx, Env.OldestTransaction, out _);

				var firstFreePage = Env.FreeSpaceHandling.Find(tx, 2);

				Assert.False(Env.FreeSpaceHandling.IsFree(tx, 4));
				Assert.False(Env.FreeSpaceHandling.IsFree(tx, 5));
				Assert.True(Env.FreeSpaceHandling.IsFree(tx, 6));
			}
		}

		[Fact]
		public void WillGetFirstRangeWithEnoughFreePages()
		{
			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				Env.FreeSpaceHandling.RegisterFreePages(tx, new List<long> {1, 4, 5, 7, 8, 9});
				List<long> _;
				Env.FreeSpaceHandling.OnCommit(tx, Env.OldestTransaction, out _);

				var firstFreePage = Env.FreeSpaceHandling.Find(tx, 2);
				Assert.Equal(4, firstFreePage);

				firstFreePage = Env.FreeSpaceHandling.Find(tx, 1);
				Assert.Equal(7, firstFreePage);

				firstFreePage = Env.FreeSpaceHandling.Find(tx, 2);
				Assert.Equal(8, firstFreePage);
			}
		}

		[Fact]
		public void MustNotReturnLessFreePagesThanRequested()
		{
			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				Env.FreeSpaceHandling.RegisterFreePages(tx, new List<long> {2, 3});
				List<long> _;
				Env.FreeSpaceHandling.OnCommit(tx, Env.OldestTransaction, out _);

				// 2 pages are free but we request for 3
				var firstFreePage = Env.FreeSpaceHandling.Find(tx, 3);
				Assert.Equal(-1, firstFreePage);
			}
		}

		[Fact]
		public void MustNotMergeFreePagesFromEndAndBegin()
		{
			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				Env.FreeSpaceHandling.RegisterFreePages(tx, new List<long> {0, 6, 7, 8, 9});
				List<long> _;
				Env.FreeSpaceHandling.OnCommit(tx, Env.OldestTransaction, out _);

				var firstFreePage = Env.FreeSpaceHandling.Find(tx, 2);
				Assert.Equal(6, firstFreePage);

				firstFreePage = Env.FreeSpaceHandling.Find(tx, 3);
				Assert.Equal(-1, firstFreePage);
			}
		}

		[Fact]
		public void WhenBufferIsDirtyShouldCopySecondBufferBeforeCanProcess()
		{
			using (var tx1 = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				Env.FreeSpaceHandling.RegisterFreePages(tx1, new List<long> {1, 3});
				tx1.Commit();
			}

			using (var tx2 = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				Env.FreeSpaceHandling.RegisterFreePages(tx2, new List<long> {4, 7, 9});
				// Env.FreeSpaceHandling.OnCommit(); - do not commit, so buffer will get marked as dirty

				Env.FreeSpaceHandling.SetBufferForTransaction(tx2); // should copy dirty pages from a buffer for tx1

				// the same free pages as in buffer for tx1
				Assert.True(Env.FreeSpaceHandling.IsFree(tx2, 1));
				Assert.True(Env.FreeSpaceHandling.IsFree(tx2, 3));

				Assert.False(Env.FreeSpaceHandling.IsFree(tx2, 4));
				Assert.False(Env.FreeSpaceHandling.IsFree(tx2, 7));
				Assert.False(Env.FreeSpaceHandling.IsFree(tx2, 9));
			}
		}

		[Fact]
		public void SearchingFreePagesShouldWorkInCycle()
		{
			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				Env.FreeSpaceHandling.RegisterFreePages(tx, new List<long> {6, 7, 8, 9});
				List<long> _;
				Env.FreeSpaceHandling.OnCommit(tx, Env.OldestTransaction, out _);

				var firstFreePage = Env.FreeSpaceHandling.Find(tx, 2);
				Assert.Equal(6, firstFreePage);

				// free pages from the beginning
				Env.FreeSpaceHandling.RegisterFreePages(tx, new List<long> {0, 1});
				Env.FreeSpaceHandling.OnCommit(tx, Env.OldestTransaction, out _);

				// move search pointer at the end
				firstFreePage = Env.FreeSpaceHandling.Find(tx, 2);
				Assert.Equal(8, firstFreePage);

				// should take from the begin
				firstFreePage = Env.FreeSpaceHandling.Find(tx, 2);
				Assert.Equal(0, firstFreePage);
			}
		}
	}
}