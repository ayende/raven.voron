// -----------------------------------------------------------------------
//  <copyright file="BinaryFreeSpaceRepository.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Collections.Generic;
using Voron.Trees;

namespace Voron.Impl.FreeSpace
{
	public class BinaryFreeSpaceRepository : IFreeSpaceRepository
	{
		private readonly BinaryFreeSpaceStrategy strategy;

		public BinaryFreeSpaceRepository(BinaryFreeSpaceStrategy strategy)
		{
			this.strategy = strategy;
		}

		public Page TryAllocateFromFreeSpace(Transaction tx, int num)
		{
			strategy.SetBufferForTransaction(tx.Id);

			var page = strategy.Find(num);

			if (page == -1)
				return null;

			var newPage = tx.Pager.Get(tx, page);
			newPage.PageNumber = page;
			return newPage;
		}

		public long GetFreePageCount()
		{
			return 0; // TODO arek
		}

		public void FlushFreeState(Transaction transaction)
		{
			
		}

		public void LastTransactionPageUsage(int pages)
		{
			
		}

		public List<long> AllPages(Transaction tx)
		{
			return null; //TODO arek
		}

		public void RegisterFreePages(Slice slice, long transactionId, List<long> freedPages)
		{
			strategy.ReleasePages(freedPages);
		}

		public void UpdateSections(Transaction tx, long oldestTransaction)
		{
		}

		public int MinimumFreePagesInSection { get; set; }
	}
}