// -----------------------------------------------------------------------
//  <copyright file="FreePagesRepositoryTests.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.IO;
using Voron.Impl;
using Xunit;

namespace Voron.Tests.Impl
{
	public class FreePagesRepositoryTests : IDisposable
	{
		[Fact]
		public void InitiallyAllTheBitsAreCleared()
		{
			using (var freeSpace = new FreePagesRepository("free-space", 8))
			{
				for (int i = 0; i < freeSpace.NumberOfTrackedPages; i++)
				{
					Assert.False(freeSpace.Buffers[0].AllBits[i]);
					Assert.False(freeSpace.Buffers[1].AllBits[i]);
				}
			}
		}

		[Fact]
		public void AddingFreePagesWillSetBitsAtCorrectPositions()
		{
			using (var freePages = new FreePagesRepository("free-space", 4))
			{
				const int transactionNumber = 1;

				freePages.Add(transactionNumber, 1);
				freePages.Add(transactionNumber, 3);

				var bitBuffer = freePages.GetBufferForTransaction(transactionNumber);

				Assert.Equal(bitBuffer.Pages[0], false);
				Assert.Equal(bitBuffer.Pages[1], true);
				Assert.Equal(bitBuffer.Pages[2], false);
				Assert.Equal(bitBuffer.Pages[3], true);
			}
		}

		[Fact]
		public void ConsecutiveTransactionsShouldUseBuffersAlternately()
		{
			using (var freePages = new FreePagesRepository("free-space", 4))
			{
				freePages.Add(0, 0);
				freePages.Add(1, 1);
				freePages.Add(2, 2);
				freePages.Add(3, 3);

				Assert.Equal(freePages.Buffers[0].Pages[0], true);
				Assert.Equal(freePages.Buffers[0].Pages[1], false);
				Assert.Equal(freePages.Buffers[0].Pages[2], true);
				Assert.Equal(freePages.Buffers[0].Pages[3], false);

				Assert.Equal(freePages.Buffers[1].Pages[0], false);
				Assert.Equal(freePages.Buffers[1].Pages[1], true);
				Assert.Equal(freePages.Buffers[1].Pages[2], false);
				Assert.Equal(freePages.Buffers[1].Pages[3], true);
			}
		}

		[Fact]
		public void ShouldNotFindAnyFreePagesWhenThereAreNone()
		{
			using (var freePages = new FreePagesRepository("free-space", 4))
			{
				var numbersOfFreePages = freePages.Find(1, 2);

				Assert.Null(numbersOfFreePages);
			}
		}

		[Fact]
		public void CanGetFreePagesFromTheBegin()
		{
			using (var freePages = new FreePagesRepository("free-space", 4))
			{
				const int transaction = 1;

				freePages.Add(transaction, 0);
				freePages.Add(transaction, 1);

				var numbersOfFreePages = freePages.Find(transaction, 2);

				Assert.Equal(2, numbersOfFreePages.Count);
				Assert.Equal(0, numbersOfFreePages[0]);
				Assert.Equal(1, numbersOfFreePages[1]);
			}
		}

		[Fact]
		public void CanGetFreePagesFromTheEnd()
		{
			using (var freePages = new FreePagesRepository("free-space", 4))
			{
				const int transaction = 1;

				freePages.Add(transaction, 2);
				freePages.Add(transaction, 3);

				var numbersOfFreePages = freePages.Find(transaction, 2);

				Assert.Equal(2, numbersOfFreePages.Count);
				Assert.Equal(2, numbersOfFreePages[0]);
				Assert.Equal(3, numbersOfFreePages[1]);
			}
		}

		[Fact]
		public void CanGetFreePagesFromTheMiddleEnd()
		{
			using (var freePages = new FreePagesRepository("free-space", 4))
			{
				const int transaction = 1;

				freePages.Add(transaction, 1);
				freePages.Add(transaction, 2);

				var numbersOfFreePages = freePages.Find(transaction, 2);

				Assert.Equal(2, numbersOfFreePages.Count);
				Assert.Equal(1, numbersOfFreePages[0]);
				Assert.Equal(2, numbersOfFreePages[1]);
			}
		}

		[Fact]
		public void GettingFreePagesShouldMarkThemAsBusy()
		{
			using (var freePages = new FreePagesRepository("free-space", 10))
			{
				const int transaction = 1;

				freePages.Add(transaction, 4);
				freePages.Add(transaction, 5);
				freePages.Add(transaction, 6);

				freePages.Find(transaction, 2);

				var pages = freePages.GetBufferForTransaction(transaction).Pages;

				Assert.False(pages[4]); // should mark as busy
				Assert.False(pages[5]); // should mark as busy
				Assert.True(pages[6]); // should remain free
			}
		}

		[Fact]
		public void WillGetFirstRangeWithEnoughFreePages()
		{
			using (var freePages = new FreePagesRepository("free-space", 10))
			{
				const int transaction = 1;

				freePages.Add(transaction, 1);

				freePages.Add(transaction, 4);
				freePages.Add(transaction, 5);

				freePages.Add(transaction, 7);
				freePages.Add(transaction, 8);
				freePages.Add(transaction, 9);

				var numbersOfFreePages = freePages.Find(transaction, 2);

				Assert.Equal(2, numbersOfFreePages.Count);
				Assert.Equal(4, numbersOfFreePages[0]);
				Assert.Equal(5, numbersOfFreePages[1]);

				numbersOfFreePages = freePages.Find(transaction, 1);

				Assert.Equal(1, numbersOfFreePages.Count);
				Assert.Equal(7, numbersOfFreePages[0]);

				numbersOfFreePages = freePages.Find(transaction, 2);

				Assert.Equal(2, numbersOfFreePages.Count);
				Assert.Equal(8, numbersOfFreePages[0]);
				Assert.Equal(9, numbersOfFreePages[1]);
			}
		}

		[Fact]
		public void MustNotReturnLessFreePagesThanRequested()
		{
			using (var freePages = new FreePagesRepository("free-space", 5))
			{
				const int transaction = 1;

				freePages.Add(transaction, 2);
				freePages.Add(transaction, 3);

				// 2 pages are free but we request for 3
				var numbersOfFreePages = freePages.Find(transaction, 3);

				Assert.Null(numbersOfFreePages);
			}
		}

		[Fact]
		public void MustNotMergeFreePagesFromEndAndBegin()
		{
			using (var freePages = new FreePagesRepository("free-space", 10))
			{
				const int transaction = 1;

				freePages.Add(transaction, 7);
				freePages.Add(transaction, 8);
				freePages.Add(transaction, 9);

				var numbersOfFreePages = freePages.Find(transaction, 1); // move searching index

				Assert.Equal(1, numbersOfFreePages.Count);
				Assert.Equal(7, numbersOfFreePages[0]);

				freePages.Add(transaction, 0); // free page 0

				numbersOfFreePages = freePages.Find(transaction, 3);
				Assert.Null(numbersOfFreePages);
			}
		}

		[Fact]
		public void WhenBufferIsDirtyShouldCopySecondBufferBeforeCanProcess()
		{
			using (var freePages = new FreePagesRepository("free-space", 10))
			{
				var tx1 = 1;
				var tx2 = 2;

				freePages.Add(tx1, 1);
				freePages.Add(tx1, 3);

				freePages.Add(tx1, 4);
				freePages.Add(tx1, 7);
				freePages.Add(tx1, 9);

				var buffer1 = freePages.GetBufferForTransaction(tx1);
				var buffer2 = freePages.GetBufferForTransaction(tx2);

				buffer1.IsDirty = true; // force as dirty so next get should return a clean buffer which will be a copy of a second buffer

				buffer1 = freePages.GetBufferForTransaction(tx1);

				Assert.False(buffer1.IsDirty);
				Assert.Equal(buffer1.AllBits.Size, buffer2.AllBits.Size);


				for (int i = 0; i < buffer1.Pages.Size; i++)
				{
					Assert.Equal(buffer1.Pages[i], buffer2.Pages[i]);
				}
			}
		}

		[Fact]
		public void CanSetPageAsModified()
		{
			using (var freePages = new FreePagesRepository("free-space", 10))
			{
				var tx = 1;

				freePages.SetModified(tx, 1);
				freePages.SetModified(tx, 5);
				freePages.SetModified(tx, 6);

				var buffer = freePages.GetBufferForTransaction(tx);

				Assert.True(buffer.ModifiedPages[1]);
				Assert.True(buffer.ModifiedPages[5]);
				Assert.True(buffer.ModifiedPages[6]);
			}
		}

		private void DeleteFiles()
		{
			if (File.Exists("free-space-0"))
				File.Delete("free-space-0");

			if (File.Exists("free-space-1"))
				File.Delete("free-space-1");
		}

		public void Dispose()
		{
			DeleteFiles();
		}
	}
}