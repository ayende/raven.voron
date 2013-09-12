using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using Voron.Impl.FileHeaders;
using Voron.Trees;

namespace Voron.Impl.FreeSpace
{
	public class BinaryFreeSpaceStrategy
	{
		public class FreeSpaceInfo
		{
			private readonly BinaryFreeSpaceStrategy _strategy;

			public FreeSpaceInfo(BinaryFreeSpaceStrategy strategy)
			{
				_strategy = strategy;
			}

		    public long FreePagesCount
		    {
		        get
		        {
		            var bits = _strategy.bits.First(x => x.IsDirty == false);
		            return bits.TotalNumberOfFreePages;
		        }
		    }

		    public List<long> GetBuffersPages()
			{
				var range = new List<long>();

				var buffer1 = _strategy.bits[0];
				var buffer2 = _strategy.bits[1];

				for (var i = buffer1.StartPageNumber; i < buffer1.StartPageNumber + _strategy.state.NumberOfPagesTakenForTracking; i++)
				{
					range.Add(i);
				}

				for (var i = buffer2.StartPageNumber; i < buffer2.StartPageNumber + _strategy.state.NumberOfPagesTakenForTracking; i++)
				{
					range.Add(i);
				}

				return range;
			}

			public List<long> GetFreePages(long transactionNumber)
			{
			    var buffer = _strategy.bits[transactionNumber & 1]; // take buffer specific for transaction

			    var result = new List<long>();

			    for (var i = 0; i < buffer.NumberOfTrackedPages; i++)
			    {
			        if (buffer.IsFree(i))
			            result.Add(i);
			    }

			    return result;
			}
		}

		public class FreedTransaction
		{
			public long Id;
			public List<long> Pages;
		}

		private readonly Func<long, IntPtr> acquirePagePointer;
		private readonly UnmanagedBits[] bits = new UnmanagedBits[2];
		private readonly LinkedList<FreedTransaction> registeredFreedPages = new LinkedList<FreedTransaction>();

		private FreeSpaceHeader state;
		private bool initialized;
		private FreeSpaceInfo _info;

		public long MaxNumberOfPages
		{
			get
			{
				Debug.Assert((bits[0] == null && bits[1] == null) || (bits[0].MaxNumberOfPages == bits[1].MaxNumberOfPages));

				return bits[0] != null ? bits[0].MaxNumberOfPages : 0;
			}
		}

		public decimal NumberOfTrackedPages
		{
			get
			{
				Debug.Assert((bits[0] == null && bits[1] == null) || (bits[0].NumberOfTrackedPages == bits[1].NumberOfTrackedPages));

				return bits[0] != null ? bits[0].NumberOfTrackedPages : 0;
			}
		}

		public FreeSpaceInfo Info
		{
			get { return _info ?? (_info = new FreeSpaceInfo(this)); }
		}

		public BinaryFreeSpaceStrategy(Func<long, IntPtr> acquirePagePointer)
		{
			this.acquirePagePointer = acquirePagePointer;
		}

		public unsafe void Initialize(FreeSpaceHeader* header)
		{
			bits[0] = new UnmanagedBits((byte*) acquirePagePointer(header->FirstBufferPageNumber).ToPointer(),
			                            header->FirstBufferPageNumber, header->NumberOfPagesTakenForTracking*header->PageSize,
			                            header->NumberOfTrackedPages, header->PageSize);
			bits[0].RefreshNumberOfFreePages();

			bits[1] = new UnmanagedBits((byte*) acquirePagePointer(header->SecondBufferPageNumber).ToPointer(),
			                            header->SecondBufferPageNumber, header->NumberOfPagesTakenForTracking*header->PageSize,
			                            header->NumberOfTrackedPages, header->PageSize);
			bits[1].RefreshNumberOfFreePages();

			state = new FreeSpaceHeader
				{
					FirstBufferPageNumber = header->FirstBufferPageNumber,
					SecondBufferPageNumber = header->SecondBufferPageNumber,
					NumberOfPagesTakenForTracking = header->NumberOfPagesTakenForTracking,
					NumberOfTrackedPages = header->NumberOfTrackedPages,
					PageSize = header->PageSize
				};

			initialized = true;
		}

		public unsafe void UpdateBufferPointers()
		{
			bits[0].SetBufferPointer((byte*)acquirePagePointer(state.FirstBufferPageNumber).ToPointer());
			bits[1].SetBufferPointer((byte*)acquirePagePointer(state.SecondBufferPageNumber).ToPointer());
		}

		public void RecoverBuffers()
		{
			if(bits.All(x => x.IsDirty == false))
				return;

			if(bits.All(x => x.IsDirty))
				throw new InvalidDataException(
						"Both buffers are dirty. Valid state of the free pages buffer cannot be restored during buffers recovery"); // should never happen

			var dirtyBuffer = bits.First(x => x.IsDirty);
			var cleanBuffer = bits.First(x => x.IsDirty == false);

			cleanBuffer.CopyAllTo(dirtyBuffer);
		}

		public UnmanagedBits GetBufferForNewTransaction(long txId)
		{
			var indexOfBuffer = txId & 1;

			var next = bits[indexOfBuffer];
			var reference = bits[1 - indexOfBuffer];

			if (next.IsDirty)
			{
				if (reference.IsDirty)
					throw new InvalidDataException(
						"Both buffers are dirty. Valid state of the free pages buffer cannot be restored. Transaction number: " +
						txId); // should never happen

				// last transaction was aborted and its buffer is dirty
				// we have to copy pages according to modification bits from dirty buffer

				Debug.Assert(next.ModificationBitsInUse == reference.ModificationBitsInUse);

				reference.CopyDirtyBitsTo(next, next.GetModificationBits());
			}
			else // copy dirty bits from reference
			{
				reference.CopyDirtyBitsTo(next);
			}
			
			next.ResetModifiedPages();

			return next;
		}

		public unsafe void MoveTo(long firstBufferPageStart, long secondBufferPageStart, long numberOfPagesToTrack, long numberOfPagesForTracking, int pageSize)
		{
			var newFirstBuffer = new UnmanagedBits((byte*) acquirePagePointer(firstBufferPageStart),
			                                       firstBufferPageStart,
			                                       numberOfPagesForTracking*pageSize,
			                                       numberOfPagesToTrack,
			                                       pageSize);

			bits[0].CopyAllTo(newFirstBuffer); // we cannot just copy because a new buffer is bigger
			bits[0] = newFirstBuffer;

			var newSecondBuffer = new UnmanagedBits((byte*) acquirePagePointer(secondBufferPageStart),
			                                        secondBufferPageStart,
			                                        numberOfPagesForTracking*pageSize,
			                                        numberOfPagesToTrack,
			                                        pageSize);
			bits[1].CopyAllTo(newSecondBuffer);
			bits[1] = newSecondBuffer;

			// mark pages that was taken by old buffers as free
			var oldHeader = state;

			bits[0].MarkPages(oldHeader.FirstBufferPageNumber, oldHeader.NumberOfPagesTakenForTracking, true);
			bits[1].MarkPages(oldHeader.FirstBufferPageNumber, oldHeader.NumberOfPagesTakenForTracking, true);

			bits[0].MarkPages(oldHeader.SecondBufferPageNumber, oldHeader.NumberOfPagesTakenForTracking, true);
			bits[1].MarkPages(oldHeader.SecondBufferPageNumber, oldHeader.NumberOfPagesTakenForTracking, true);

			bits[0].Processed();
			bits[1].Processed();

			// update header
			state = new FreeSpaceHeader
			{
				FirstBufferPageNumber = firstBufferPageStart,
				SecondBufferPageNumber = secondBufferPageStart,
				NumberOfTrackedPages = numberOfPagesToTrack,
				NumberOfPagesTakenForTracking = numberOfPagesForTracking,
				PageSize = pageSize
			};
		}

		public void RegisterFreePages(Transaction tx, List<long> freedPages)
		{
			registeredFreedPages.AddLast(new FreedTransaction()
				{
					Id = tx.Id,
					Pages = freedPages
				});
		}

		public void OnCommit(UnmanagedBits buffer, long oldestTx, out List<long> dirtyPages)
		{
			if (buffer == null)
			{
				dirtyPages = new List<long>();
				return;
			}

			while (registeredFreedPages.First != null && registeredFreedPages.First.Value.Id <= oldestTx)
			{
				var val = registeredFreedPages.First.Value;
				registeredFreedPages.RemoveFirst();

				foreach (var freedPage in val.Pages)
				{
					buffer.MarkPage(freedPage, true);
				}

				buffer.TotalNumberOfFreePages += val.Pages.Count;
			}

			dirtyPages = buffer.DirtyPages;

			buffer.Processed();
		}

		public unsafe void CopyStateTo(FreeSpaceHeader* freeSpaceHeader)
		{
			freeSpaceHeader->FirstBufferPageNumber = state.FirstBufferPageNumber;
			freeSpaceHeader->SecondBufferPageNumber = state.SecondBufferPageNumber;
			freeSpaceHeader->NumberOfTrackedPages = state.NumberOfTrackedPages;
			freeSpaceHeader->NumberOfPagesTakenForTracking = state.NumberOfPagesTakenForTracking;
			freeSpaceHeader->PageSize = state.PageSize;
		}

		public void TrackMorePages(long newNumberOfPagesToTrack)
		{
			foreach (var unmanagedBits in bits)
			{
				unmanagedBits.IncreaseSize(newNumberOfPagesToTrack);
			}
		}
	}
}