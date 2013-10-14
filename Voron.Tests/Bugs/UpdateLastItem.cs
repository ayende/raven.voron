using System.Collections.Generic;
using System.IO;
using Voron.Impl;
using Voron.Impl.FileHeaders;
using Xunit;

namespace Voron.Tests.Bugs
{
	public unsafe class UpdateLastItem : StorageTest
	{
		[Fact]
		public void ShouldWork()
		{
			using (var pager = new PureMemoryPager())
			{
				var logPagers = new Dictionary<string, PureMemoryPager>();

				using (var env = new StorageEnvironment(pager,
				                                        logName =>
					                                        {
						                                        var p = new PureMemoryPager();
						                                        logPagers[logName] = p;
						                                        return p;
					                                        }, 
														ownsPagers: false))
				{
					using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
					{
						env.Root.DirectAdd(tx, "events", sizeof (TreeRootHeader));
						env.Root.DirectAdd(tx, "aggregations", sizeof (TreeRootHeader));
						env.Root.DirectAdd(tx, "aggregation-status", sizeof (TreeRootHeader));
						tx.Commit();
					}
					using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
					{
						env.Root.DirectAdd(tx, "events", sizeof (TreeRootHeader));

						tx.Commit();
					}
				}

				using (var env = new StorageEnvironment(pager, 
														logName =>
															{
																if (logPagers.ContainsKey(logName))
																	return logPagers[logName];
																return new PureMemoryPager();
															}, 
														ownsPagers: false))
				using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
				{
					env.Root.DirectAdd(tx, "events", sizeof (TreeRootHeader));

					tx.Commit();
				}
			}
		}
	}
}