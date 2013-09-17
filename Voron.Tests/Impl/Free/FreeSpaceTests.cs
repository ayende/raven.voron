// -----------------------------------------------------------------------
//  <copyright file="BinaryFreeSpaceStrategyTests.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using Voron.Impl.FreeSpace;
using Xunit;

namespace Voron.Tests.Impl.Free
{
	public class FreeSpaceTests : StorageTest
	{
		[Fact]
		public void ShouldNotFindAnyFreePagesWhenThereAreNone()
		{
			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				var freePageStart = tx.FreeSpaceBuffer.Find(1);

				Assert.Equal(-1, freePageStart);
			}
		}

		[Fact]
		public void CanGetFreePagesFromTheBegin()
		{
			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				tx.FreePage(4);
				tx.FreePage(5);

				tx.Commit();

				var firstFreePage = tx.FreeSpaceBuffer.Find(2);

				Assert.Equal(4, firstFreePage);
			}
		}

		[Fact]
		public void CanGetFreePagesFromTheEnd()
		{
			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				var numberOfTrackedPages = (long)Env.FreeSpaceHandling.NumberOfTrackedPages;

				tx.FreePage(numberOfTrackedPages - 1);
				tx.FreePage(numberOfTrackedPages - 2);

				tx.Commit();

				var firstFreePage = tx.FreeSpaceBuffer.Find(2);

				Assert.Equal(numberOfTrackedPages - 2, firstFreePage);
			}
		}

		[Fact]
		public void CanGetFreePagesFromTheMiddle()
		{
			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				tx.FreePage(5);
				tx.FreePage(6);

				tx.Commit();

				var firstFreePage = tx.FreeSpaceBuffer.Find(2);

				Assert.Equal(5, firstFreePage);
			}
		}

		[Fact]
		public void GettingFreePagesShouldMarkThemAsBusy()
		{
			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				tx.FreePage(4);
				tx.FreePage(5);
				tx.FreePage(6);

				tx.Commit();

				var firstFreePage = tx.FreeSpaceBuffer.Find(2);

				Assert.False(tx.FreeSpaceBuffer.IsFree(4));
				Assert.False(tx.FreeSpaceBuffer.IsFree(5));
				Assert.True(tx.FreeSpaceBuffer.IsFree(6));
			}
		}

		[Fact]
		public void WillGetFirstRangeWithEnoughFreePages()
		{
			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				tx.FreePage(2);

				tx.FreePage(4);
				tx.FreePage(5);
				tx.FreePage(7);

				tx.FreePage(8);
				tx.FreePage(9);

				tx.Commit();

				var firstFreePage = tx.FreeSpaceBuffer.Find(2);
				Assert.Equal(4, firstFreePage);

				firstFreePage = tx.FreeSpaceBuffer.Find(1);
				Assert.Equal(7, firstFreePage);

				firstFreePage = tx.FreeSpaceBuffer.Find(2);
				Assert.Equal(8, firstFreePage);
			}
		}

		[Fact]
		public void MustNotReturnLessFreePagesThanRequested()
		{
			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				tx.FreePage(2);
				tx.FreePage(3);

				tx.Commit();

				// 2 pages are free but we request for 3
				var firstFreePage = tx.FreeSpaceBuffer.Find(3);
				Assert.Equal(-1, firstFreePage);
			}
		}

		[Fact]
		public void MustNotMergeFreePagesFromEndAndBegin()
		{
			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				var numberOfTrackedPages = (long)Env.FreeSpaceHandling.NumberOfTrackedPages;

				tx.FreePage(2);

				tx.FreePage(numberOfTrackedPages - 3);
				tx.FreePage(numberOfTrackedPages - 2);
				tx.FreePage(numberOfTrackedPages - 1);
				tx.FreePage(numberOfTrackedPages);

				tx.Commit();

				var firstFreePage = tx.FreeSpaceBuffer.Find(2);
				Assert.Equal(numberOfTrackedPages - 3, firstFreePage);

				firstFreePage = tx.FreeSpaceBuffer.Find(3);
				Assert.Equal(-1, firstFreePage);
			}
		}

		[Fact]
		public void WhenBufferIsDirtyShouldCopyDirtyPagesFromCleanBufferBeforeCanProcess()
		{
			using (var tx1 = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				tx1.FreePage(2);
				tx1.FreePage(4);

				tx1.Commit();
			}

			using (var tx2 = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				tx2.FreeSpaceBuffer.Find(1); // will mark buffer as dirty

				tx2.FreePage(5);
				tx2.FreePage(6);
				tx2.FreePage(7);

				// tx2.Commit(); - do not commit, so buffer will remain marked as dirty

				Assert.True(tx2.FreeSpaceBuffer.IsDirty);

				Env.FreeSpaceHandling.GetBufferForNewTransaction(tx2.Id); // should copy dirty pages from a buffer for tx1

				// the same free pages as in buffer for tx1
				Assert.True(tx2.FreeSpaceBuffer.IsFree(2));
				Assert.True(tx2.FreeSpaceBuffer.IsFree(4));

				Assert.False(tx2.FreeSpaceBuffer.IsFree(5));
				Assert.False(tx2.FreeSpaceBuffer.IsFree(6));
				Assert.False(tx2.FreeSpaceBuffer.IsFree(7));
			}
		}

		[Fact]
		public void SearchingFreePagesShouldWorkInCycle()
		{
			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				tx.FreePage(6);
				tx.FreePage(7);
				tx.FreePage(8);
				tx.FreePage(9);

				tx.Commit();

				var firstFreePage = tx.FreeSpaceBuffer.Find(2);
				Assert.Equal(6, firstFreePage);

				// free pages from the beginning
				tx.FreePage(2);
				tx.FreePage(3);

				tx.Commit();

				// move search pointer at the end
				firstFreePage = tx.FreeSpaceBuffer.Find(2);
				Assert.Equal(8, firstFreePage);

				// should take from the begin
				firstFreePage = tx.FreeSpaceBuffer.Find(2);
				Assert.Equal(2, firstFreePage);
			}
		}

		[Fact]
		public void LastSearchingPositionShouldBeSharedBetweenBuffers()
		{
			using (var tx1 = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				tx1.FreePage(6);
				tx1.FreePage(7);
				tx1.FreePage(8);
				tx1.FreePage(9);

				tx1.Commit();

				tx1.Commit();
			}

			using (var tx2 = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				tx2.FreeSpaceBuffer.Find(2);

				tx2.FreePage(2);
				tx2.FreePage(3);

				tx2.Commit();
			}

			using (var tx3 = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				var start = tx3.FreeSpaceBuffer.Find(2);

				Assert.Equal(8, start);
			}
		}

		[Fact]
		public void ShouldTakeBuffersAlternately()
		{
			UnmanagedBits tx1Buffer, tx2Buffer, tx3Buffer, tx4Buffer;
			using (var tx1 = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				tx1Buffer = tx1.FreeSpaceBuffer;
				tx1.Commit();
			}
			using (var tx2 = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				tx2Buffer = tx2.FreeSpaceBuffer;
				tx2.Commit();
			}
			using (var tx3 = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				tx3Buffer = tx3.FreeSpaceBuffer;
				tx3.Commit();
			}
			using (var tx4 = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				tx4Buffer = tx4.FreeSpaceBuffer;
				tx4.Commit();
			}

			Assert.Same(tx1Buffer, tx3Buffer);
			Assert.Same(tx2Buffer, tx4Buffer);
			Assert.NotSame(tx1Buffer, tx2Buffer);
		}

		[Fact]
		public void FreeSpaceChangesShouldBeVisibleBetweenTransactions()
		{
			using (var tx1 = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				tx1.FreePage(4);
				tx1.FreePage(5);
				tx1.Commit();
			}

			using (var tx2 = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				var page = tx2.FreeSpaceBuffer.Find(2);
				Assert.Equal(4, page);
			}
		}

		[Fact]
		public void ShouldFreePagesOnlyWhenAreAreNoOlderConcurrentReadTransactions()
		{
			using (var tx1 = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				tx1.FreePage(3);

				tx1.Commit();
			}

			Assert.Equal(1, Env.Stats().FreePages);

			using (var tx2 = Env.NewTransaction(TransactionFlags.Read))
			{
				using (var tx3 = Env.NewTransaction(TransactionFlags.ReadWrite))
				{
					tx3.FreePage(4);

					tx3.Commit();
				}

				Assert.Equal(1, Env.Stats().FreePages);

				tx2.Commit();
			}

			using (var tx4 = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				tx4.Commit();
			}

			Assert.Equal(2, Env.Stats().FreePages);
		}

		[Fact]
		public void FreeSpaceBufferOfAbortedTransactionShouldBeRecovered()
		{
			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				tx.Pager.AllocateMorePages(tx, 1024 * 1024 * 5);
				tx.Pager.EnsureContinuous(tx, 800000, 1);
				tx.Commit();
			}

			using (var tx1 = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				tx1.FreePage(100);
				tx1.FreePage(80000);
				tx1.FreePage(80001);
				tx1.FreePage(80002);
				tx1.FreePage(80003);
				tx1.Commit();
			}

			UnmanagedBits bufferOfTx2;

			using (var tx2 = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				bufferOfTx2 = tx2.FreeSpaceBuffer;

				var find = tx2.FreeSpaceBuffer.Find(4);
				Assert.Equal(80000, find); // it would mark pages as busy
				
				// do not commit - aborted transaction
			}

			using (var tx3 = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				// this transaction will get the same id as the previous one because it was aborted
				// so here we should get the recovered buffer

				Assert.Same(bufferOfTx2, tx3.FreeSpaceBuffer);

				Assert.True(tx3.FreeSpaceBuffer.IsFree(100));
				Assert.True(tx3.FreeSpaceBuffer.IsFree(80000));
				Assert.True(tx3.FreeSpaceBuffer.IsFree(80001));
				Assert.True(tx3.FreeSpaceBuffer.IsFree(80002));
				Assert.True(tx3.FreeSpaceBuffer.IsFree(80003));
				tx3.Commit();
			}
		}
	}
}