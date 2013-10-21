// -----------------------------------------------------------------------
//  <copyright file="EdgeCases.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.IO;
using System.Linq;
using Voron.Impl;
using Xunit;

namespace Voron.Tests.Log
{
	public class EdgeCases : StorageTest
	{
		// all tests here relay on the fact than one log file can contains max 10 pages
		protected override void Configure(StorageOptions options, IVirtualPager pager)
		{
			options.LogFileSize = 10 * pager.PageSize;
		}

		[Fact]
		public void TransactionCommitShouldScheduleFlushToDataFileForFullLogAndSetItNull()
		{
			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				var bytes = new byte[5 * Env.PageSize]; // 5 pages should fill in the log file completely
				Env.Root.Add(tx, "items/0", new MemoryStream(bytes));
				tx.Commit();
			}

			Assert.Null(Env.Log._currentFile);
			Assert.Equal(1, Env.Log._scheduledToFlush.Count);
			Assert.Equal(0, Env.Log._scheduledToFlush.First().Number);
		}
	}
}