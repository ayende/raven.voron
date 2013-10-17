using System.Collections.Generic;
using System.IO;
using Voron.Impl;
using Xunit;

namespace Voron.Tests.Bugs
{
	public class EmptyTree : StorageTest
	{
		 [Fact]
		 public void ShouldBeEmpty()
		 {
			 using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			 {
				 Env.CreateTree(tx, "events");

				 tx.Commit();
			 }

			 using (var tx = Env.NewTransaction(TransactionFlags.Read))
			 {
				 var treeIterator = Env.GetTree(tx, "events").Iterate(tx);

				 Assert.False(treeIterator.Seek(Slice.AfterAllKeys));

				 tx.Commit();
			 }
		 }

		 [Fact]
		 public void SurviveRestart()
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
				                                         new StorageOptions()
					                                         {
						                                         OwnsPagers = false
					                                         }))
				 {
					 using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
					 {
						 env.CreateTree(tx, "events");

						 tx.Commit();
					 }

					 using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
					 {
						 env.GetTree(tx, "events").Add(tx, "test", new MemoryStream(0));

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
				                                         new StorageOptions()))
				 {
					 using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
					 {
						 env.CreateTree(tx, "events");

						 tx.Commit();
					 }

					 using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
					 {
						 Assert.NotNull(env.GetTree(tx, "events").Read(tx, "test"));

						 tx.Commit();
					 }
				 }
			 }


		 }
	}
}