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

				Assert.Equal(0, numbersOfFreePages.Count);
			}
		}

		[Fact]
		public void CanGetFreePages()
		{
			using (var freePages = new FreePagesRepository("free-space", 4))
			{
				const int transaction = 1;

				freePages.Add(transaction, 3);
				freePages.Add(transaction, 2);

				var numbersOfFreePages = freePages.Find(transaction, 2);

				Assert.Equal(2, numbersOfFreePages.Count);
				Assert.Equal(2, numbersOfFreePages[0]);
				Assert.Equal(3, numbersOfFreePages[1]);
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