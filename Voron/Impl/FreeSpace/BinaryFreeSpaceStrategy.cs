using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using Voron.Impl.FileHeaders;

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
					if (_strategy._bits.All(x => x.IsDirty == false) && _strategy._lastCommittedWriteTransactionBuffer != null)
					{
						return _strategy._lastCommittedWriteTransactionBuffer.TotalNumberOfFreePages;
					}

		            var bits = _strategy._bits.First(x => x.IsDirty == false);
		            return bits.TotalNumberOfFreePages;
		        }
		    }

		    public List<long> GetBuffersPages()
			{
				var range = new List<long>();

				var buffer1 = _strategy._bits[0];
				var buffer2 = _strategy._bits[1];

				for (var i = buffer1.StartPageNumber; i < buffer1.StartPageNumber + _strategy._state.NumberOfPagesTakenForTracking; i++)
				{
					range.Add(i);
				}

				for (var i = buffer2.StartPageNumber; i < buffer2.StartPageNumber + _strategy._state.NumberOfPagesTakenForTracking; i++)
				{
					range.Add(i);
				}

				return range;
			}

			public List<long> GetFreePages(long transactionNumber)
			{
			    var buffer = _strategy._bits[transactionNumber & 1]; // take buffer specific for transaction

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

		private readonly Func<long, IntPtr> _acquirePagePointer;
		private readonly UnmanagedBits[] _bits = new UnmanagedBits[2];
		private readonly LinkedList<FreedTransaction> _registeredFreedPages = new LinkedList<FreedTransaction>();

		private FreeSpaceHeader _state;
		private FreeSpaceInfo _info;
		private UnmanagedBits _lastCommittedWriteTransactionBuffer;

		public long MaxNumberOfPages
		{
			get
			{
				Debug.Assert((_bits[0] == null && _bits[1] == null) || (_bits[0].MaxNumberOfPages == _bits[1].MaxNumberOfPages));

				return _bits[0] != null ? _bits[0].MaxNumberOfPages : 0;
			}
		}

		public decimal NumberOfTrackedPages
		{
			get
			{
				Debug.Assert((_bits[0] == null && _bits[1] == null) || (_bits[0].NumberOfTrackedPages == _bits[1].NumberOfTrackedPages));

				return _bits[0] != null ? _bits[0].NumberOfTrackedPages : 0;
			}
		}

		public FreeSpaceInfo Info
		{
			get { return _info ?? (_info = new FreeSpaceInfo(this)); }
		}

		public BinaryFreeSpaceStrategy(Func<long, IntPtr> acquirePagePointer)
		{
			_acquirePagePointer = acquirePagePointer;
		}

		public unsafe void Initialize(FreeSpaceHeader* header, bool clearBuffers = false)
		{
			_bits[0] = new UnmanagedBits((byte*) _acquirePagePointer(header->FirstBufferPageNumber).ToPointer(),
			                            header->FirstBufferPageNumber, header->NumberOfPagesTakenForTracking*header->PageSize,
			                            header->NumberOfTrackedPages, header->PageSize);
			

			_bits[1] = new UnmanagedBits((byte*) _acquirePagePointer(header->SecondBufferPageNumber).ToPointer(),
			                            header->SecondBufferPageNumber, header->NumberOfPagesTakenForTracking*header->PageSize,
			                            header->NumberOfTrackedPages, header->PageSize);
			if (clearBuffers)
			{
				_bits[0].Clear();
				_bits[1].Clear();
			}
			else
			{
				_bits[1].RefreshNumberOfFreePages();
				_bits[0].RefreshNumberOfFreePages();
			}
			
			_state = new FreeSpaceHeader
				{
					FirstBufferPageNumber = header->FirstBufferPageNumber,
					SecondBufferPageNumber = header->SecondBufferPageNumber,
					NumberOfPagesTakenForTracking = header->NumberOfPagesTakenForTracking,
					NumberOfTrackedPages = header->NumberOfTrackedPages,
					PageSize = header->PageSize,
					Checksum = header->Checksum
				};
		}

		public unsafe void UpdateBufferPointers()
		{
			_bits[0].SetBufferPointer((byte*)_acquirePagePointer(_state.FirstBufferPageNumber).ToPointer());
			_bits[1].SetBufferPointer((byte*)_acquirePagePointer(_state.SecondBufferPageNumber).ToPointer());
		}

		public void RecoverBuffers()
		{
			var firstBufferMatch = _bits[0].CalculateChecksum() == _state.Checksum;
			var secondBufferMatch = _bits[1].CalculateChecksum() == _state.Checksum;

			if(firstBufferMatch && secondBufferMatch)
				return;

			if(firstBufferMatch == false && secondBufferMatch == false)
				throw new InvalidDataException(
						"Checksum mismatch. Both buffers are in invalid state. Valid state of the free pages buffer cannot be restored during buffers recovery"); // should never happen

			var validBufferIndex = firstBufferMatch ? 0 : 1;

			var validBuffer = _bits[validBufferIndex];
			var dirtyBuffer = _bits[1 - validBufferIndex];

			validBuffer.CopyAllTo(dirtyBuffer);
		}

		public UnmanagedBits GetBufferForNewTransaction(long txId)
		{
			var indexOfBuffer = txId & 1;

			var next = _bits[indexOfBuffer];
			var reference = _bits[1 - indexOfBuffer];

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
			var newFirstBuffer = new UnmanagedBits((byte*) _acquirePagePointer(firstBufferPageStart),
			                                       firstBufferPageStart,
			                                       numberOfPagesForTracking*pageSize,
			                                       numberOfPagesToTrack,
			                                       pageSize);

			_bits[0].CopyAllTo(newFirstBuffer);
			_bits[0] = newFirstBuffer;

			var newSecondBuffer = new UnmanagedBits((byte*) _acquirePagePointer(secondBufferPageStart),
			                                        secondBufferPageStart,
			                                        numberOfPagesForTracking*pageSize,
			                                        numberOfPagesToTrack,
			                                        pageSize);

			_bits[1].CopyAllTo(newSecondBuffer);
			_bits[1] = newSecondBuffer;

			// mark pages that was taken by old buffers as free
			var oldHeader = _state;

			_bits[0].MarkPages(oldHeader.FirstBufferPageNumber, oldHeader.NumberOfPagesTakenForTracking, true);
			_bits[1].MarkPages(oldHeader.FirstBufferPageNumber, oldHeader.NumberOfPagesTakenForTracking, true);

			_bits[0].MarkPages(oldHeader.SecondBufferPageNumber, oldHeader.NumberOfPagesTakenForTracking, true);
			_bits[1].MarkPages(oldHeader.SecondBufferPageNumber, oldHeader.NumberOfPagesTakenForTracking, true);

			_bits[0].TotalNumberOfFreePages += 2*oldHeader.NumberOfPagesTakenForTracking; // for each buffer
			_bits[1].TotalNumberOfFreePages += 2*oldHeader.NumberOfPagesTakenForTracking;

			_bits[0].Processed();
			_bits[1].Processed();

			// update header
			_state = new FreeSpaceHeader
			{
				FirstBufferPageNumber = firstBufferPageStart,
				SecondBufferPageNumber = secondBufferPageStart,
				NumberOfTrackedPages = numberOfPagesToTrack,
				NumberOfPagesTakenForTracking = numberOfPagesForTracking,
				PageSize = pageSize,
				Checksum = _bits.First(x => x.IsDirty == false).CalculateChecksum()
			};
		}

		public void RegisterFreePages(Transaction tx, List<long> freedPages)
		{
			_registeredFreedPages.AddLast(new FreedTransaction
				{
					Id = tx.Id,
					Pages = freedPages
				});
		}

		public void OnTransactionCommit(Transaction tx, long oldestTx, out List<long> dirtyPages)
		{
			if (tx.FreeSpaceBuffer == null)
			{
				dirtyPages = null;
				return;
			}

			while (_registeredFreedPages.First != null && _registeredFreedPages.First.Value.Id <= oldestTx)
			{
				var val = _registeredFreedPages.First.Value;
				_registeredFreedPages.RemoveFirst();

				foreach (var freedPage in val.Pages)
				{
					tx.FreeSpaceBuffer.MarkPage(freedPage, true);
				}

				tx.FreeSpaceBuffer.TotalNumberOfFreePages += val.Pages.Count;
			}

			dirtyPages = tx.FreeSpaceBuffer.DirtyPages;

			tx.FreeSpaceBuffer.Processed();

			Debug.Assert(tx.Flags == TransactionFlags.ReadWrite);
			
			_lastCommittedWriteTransactionBuffer = tx.FreeSpaceBuffer;
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public unsafe void CopyStateTo(FreeSpaceHeader* freeSpaceHeader)
		{
			freeSpaceHeader->FirstBufferPageNumber = _state.FirstBufferPageNumber;
			freeSpaceHeader->SecondBufferPageNumber = _state.SecondBufferPageNumber;
			freeSpaceHeader->NumberOfTrackedPages = _state.NumberOfTrackedPages;
			freeSpaceHeader->NumberOfPagesTakenForTracking = _state.NumberOfPagesTakenForTracking;
			freeSpaceHeader->PageSize = _state.PageSize;
			freeSpaceHeader->Checksum = _state.Checksum;
		}

		public void TrackMorePages(long newNumberOfPagesToTrack)
		{
			foreach (var unmanagedBits in _bits)
			{
				unmanagedBits.IncreaseSize(newNumberOfPagesToTrack);
			}
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public void UpdateChecksum(Transaction tx)
		{
			if(tx.FreeSpaceBuffer == null)
				return;

			_state.Checksum = tx.FreeSpaceBuffer.CalculateChecksum();
		}
	}
}