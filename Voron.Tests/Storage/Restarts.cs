using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using Voron.Impl;
using Voron.Impl.Log;
using Xunit;

namespace Voron.Tests.Storage
{
    public class Restarts
    {
		[Fact]
		public void DataIsKeptAfterRestart()
		{
			using (var pureMemoryPager = new PureMemoryPager())
			{
				var logPagers = new Dictionary<string, PureMemoryPager>();

				using (var env = new StorageEnvironment(pureMemoryPager, logName =>
					                                        {
						                                        var pager = new PureMemoryPager();
						                                        logPagers[logName] = pager;
						                                        return pager;
					                                        },
				                                        ownsPagers: false))
				{
					using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
					{
						env.Root.Add(tx, "test/1", new MemoryStream());
						tx.Commit();
					}
					using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
					{
						env.Root.Add(tx, "test/2", new MemoryStream());
						tx.Commit();
					}
				}

				using (var env = new StorageEnvironment(pureMemoryPager, logName =>
					{
						if (logPagers.ContainsKey(logName))
							return logPagers[logName];
						return new PureMemoryPager();
					}))
				{
					using (var tx = env.NewTransaction(TransactionFlags.Read))
					{
						Assert.NotNull(env.Root.Read(tx, "test/1"));
						Assert.NotNull(env.Root.Read(tx, "test/2"));
						tx.Commit();
					}
				}
			}
		}

		//[Fact]
		//public void DataIsKeptAfterRestartForSubTrees()
		//{
		//	using (var pureMemoryPager = new PureMemoryPager())
		//	{
		//		using (var env = new StorageEnvironment(pureMemoryPager, ownsPager: false))
		//		{
		//			using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
		//			{
		//				env.CreateTree(tx, "test");
		//				tx.Commit();
		//			}
		//			using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
		//			{
		//				env.GetTree(tx,"test").Add(tx, "test", Stream.Null);
		//				tx.Commit();
		//			}
		//		}

		//		using (var env = new StorageEnvironment(pureMemoryPager))
		//		{
		//			using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
		//			{
		//				env.CreateTree(tx, "test");
		//				tx.Commit();
		//			}

		//			using (var tx = env.NewTransaction(TransactionFlags.Read))
		//			{
		//				Assert.NotNull(env.GetTree(tx,"test").Read(tx, "test"));
		//				tx.Commit();
		//			}
		//		}
		//	}
		//}

		//[Fact]
		//public void FreeSpaceBuffersAreRecoveredAfterRestartIfNecessary()
		//{
		//	using (var pureMemoryPager = new PureMemoryPager())
		//	{
		//		long totalNumberOfFreePages;

		//		using (var env = new StorageEnvironment(pureMemoryPager, ownsPager: false))
		//		{
		//			using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
		//			{
		//				env.Root.Add(tx, "test/1", new MemoryStream());
		//				tx.Commit();
		//			}

		//			using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
		//			{
		//				env.Root.Delete(tx, "test/1");
		//				tx.Commit();

		//				totalNumberOfFreePages = tx.FreeSpaceBuffer.TotalNumberOfFreePages;
		//			}

		//			using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
		//			{
		//				env.Root.Add(tx, "test/1", new MemoryStream()); // this will take one free page and mark them as busy but note that we don't commit this transaction
		//				// tx.Commit(); - intentionally do not commit, this will cause buffers checksum mismatch
		//			}
		//		}

		//		using (var env = new StorageEnvironment(pureMemoryPager))
		//		{
		//			using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
		//			{
		//				Assert.Equal(totalNumberOfFreePages, tx.FreeSpaceBuffer.TotalNumberOfFreePages);
		//				tx.Commit();
		//			}
		//			using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
		//			{
		//				Assert.Equal(totalNumberOfFreePages, tx.FreeSpaceBuffer.TotalNumberOfFreePages);
		//				tx.Commit();
		//			}
		//		}
		//	}
		//}
    }
}