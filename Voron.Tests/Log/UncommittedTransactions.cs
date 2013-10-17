// -----------------------------------------------------------------------
//  <copyright file="UncommittedTransactions.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.IO;
using Voron.Impl;
using Xunit;

namespace Voron.Tests.Log
{
	public class UncommittedTransactions : StorageTest
	{
		// all tests here relay on the fact than one log file can contains max 10 pages
		protected override void Configure(StorageOptions options, IVirtualPager pager)
		{
			options.LogFileSize = 10 * pager.PageSize;
		}

		[Fact]
		public void ShouldReusePagesOfUncommittedTransactionEvenIfItFilledTheLogCompletely()
		{
			using (var tx0 = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				var bytes = new byte[5 * Env.PageSize]; // 5 pages should fill in the log file completely
				Env.Root.Add(tx0, "items/0", new MemoryStream(bytes));
				//tx0.Commit(); intentionally
			}

			Assert.Equal(0, Env.Log._currentFile.AvailablePages);

			var writePositionAfterUncommittedTransaction = Env.Log._currentFile.WritePagePosition;

			// should reuse pages allocated by uncommitted tx0
			using (var tx1 = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				var bytes = new byte[4 * Env.PageSize]; // here we allocate less pages
				Env.Root.Add(tx1, "items/1", new MemoryStream(bytes));
				tx1.Commit();
			}

			Assert.Equal(0, Env.Log._currentFile.Number);
			Assert.True(Env.Log._currentFile.WritePagePosition < writePositionAfterUncommittedTransaction);
		}

		[Fact]
		public void UncommittedTransactionMustNotModifyPageTranslationTableOfLogFile()
		{
			long pageAllocatedInUncommittedTransaction;
			using (var tx1 = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				var page = tx1.AllocatePage(1);

				pageAllocatedInUncommittedTransaction = page.PageNumber;

				Assert.NotNull(Env.Log.ReadPage(tx1, pageAllocatedInUncommittedTransaction));
				
				// tx.Commit(); do not commit
			}
			using (var tx2 = Env.NewTransaction(TransactionFlags.Read))
			{
				// tx was not committed so in the log should not apply
				var readPage = Env.Log.ReadPage(tx2, pageAllocatedInUncommittedTransaction);

				Assert.Null(readPage);
			}
		}
	}
}