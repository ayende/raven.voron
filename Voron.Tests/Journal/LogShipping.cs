using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Voron.Impl;
using Voron.Impl.Journal;
using Xunit;

namespace Voron.Tests.Journal
{
	public class LogShipping : StorageTest
	{
		public LogShipping()
			: base(StorageEnvironmentOptions.CreateMemoryOnly())
		{			
		}

		[Fact]
		public void Committing_tx_should_fire_event_with_transactionsToShip_records()
		{
			var transactionsToShip = new List<TransactionToShip>();
			Env.Journal.OnTransactionCommit += transactionsToShip.Add;

			WriteTestDataToEnv();

			Assert.Equal(3,transactionsToShip.Count);
			Assert.Equal((uint)0, transactionsToShip[0].PreviousTransactionCrc);
			Assert.Equal(transactionsToShip[0].Header.Crc, transactionsToShip[1].PreviousTransactionCrc);
			Assert.Equal(transactionsToShip[1].Header.Crc, transactionsToShip[2].PreviousTransactionCrc);
		}

		[Fact]
		public void StorageEnvironment_should_be_able_to_accept_transactionsToShip()
		{
			var transactionsToShip = new List<TransactionToShip>();
			Env.Journal.OnTransactionCommit += transactionsToShip.Add;

			WriteTestDataToEnv();

			using (var shippingDestinationEnv = new StorageEnvironment(StorageEnvironmentOptions.CreateMemoryOnly()))
			{
				
			}
		}

		[Fact]
		public void Committed_tx_should_be_possible_to_read_from_journal_as_shipping_records()
		{
			WriteTestDataToEnv();
			var transactionsToShip = Env.Journal.Shipper.ReadJournalForShippings(-1).ToList();

			Assert.Equal(3,transactionsToShip.Count);
			Assert.Equal((uint)0, transactionsToShip[0].PreviousTransactionCrc);
			Assert.Equal(transactionsToShip[0].Header.Crc, transactionsToShip[1].PreviousTransactionCrc);
			Assert.Equal(transactionsToShip[1].Header.Crc, transactionsToShip[2].PreviousTransactionCrc);
		}

		private void WriteTestDataToEnv()
		{
			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				Env.CreateTree(tx, "TestTree");
				tx.Commit();
			}

			var writeBatch = new WriteBatch();
			writeBatch.Add("foo", StreamFor("bar"), "TestTree");

			Env.Writer.Write(writeBatch);

			writeBatch = new WriteBatch();
			writeBatch.Add("bar", StreamFor("foo"), "TestTree");

			Env.Writer.Write(writeBatch);
		}
	}
}
