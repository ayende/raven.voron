// -----------------------------------------------------------------------
//  <copyright file="FreePagesRepository.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using Voron.Util;

namespace Voron.Impl
{
	public unsafe class FreePagesRepository : IDisposable
	{
		private readonly IBufferProvider[] bufferProviders;

		public FreePagesRepository(string name, long numberOfPages)
		{
			bufferProviders = new IBufferProvider[2];
			Buffers = new BitBuffer[2];

			for (var i = 0; i < 2; i++)
			{
				bufferProviders[i] = new MemoryMapBufferProvider(string.Format("{0}-{1}", name, i));
			}

			CreateBitBuffers(numberOfPages);
		}

		public BitBuffer[] Buffers { get; private set; }

		public long NumberOfTrackedPages { get; private set; }

		private void CreateBitBuffers(long numberOfPages)
		{
			NumberOfTrackedPages = numberOfPages;

			for (int i = 0; i < 2; i++)
			{
				var size = BitBuffer.CalculateSizeInBytesForAllocation(numberOfPages);

				bufferProviders[i].IncreaseSize(size);

				bufferProviders[i].Release();

				Buffers[i] = new BitBuffer(bufferProviders[i].GetBufferPointer(), numberOfPages);
			}
		}

		public void Add(long transactionNumber, long pageNumber)
		{
			var buffer = GetBufferForTransaction(transactionNumber);
			buffer.Pages[pageNumber] = true;
		}

		public void SetModified(long transactionNumber, long pageNumber)
		{
			var buffer = GetBufferForTransaction(transactionNumber);
			buffer.ModifiedPages[pageNumber] = true;
		}

		public BitBuffer GetBufferForTransaction(long transactionNumber)
		{
			var indexOfBuffer = transactionNumber & 1;

			var selected = Buffers[indexOfBuffer];
			var reference = Buffers[1 - indexOfBuffer];

			if (selected.IsDirty)
			{
				if (reference.IsDirty)
					throw new InvalidDataException(
						"Both buffers are dirty. Valid state of the free pages buffer cannot be restored. Transaction number: " +
						transactionNumber); // should never happen

				Debug.Assert(selected.AllBits.Size == reference.AllBits.Size);

				NativeMethods.memcpy((byte*) selected.AllBits.Ptr, (byte*) reference.AllBits.Ptr,
				                     (int) UnmanagedBits.GetSizeInBytesFor(reference.AllBits.Size));
			}
			else
			{
				Debug.Assert(selected.ModifiedPages.Size == reference.ModifiedPages.Size);

				for (int i = 0; i < reference.ModifiedPages.Size; i++)
				{
					if (reference.ModifiedPages[i])
					{
						selected.Pages[i] = true;
					}
				}
			}

			return selected;
		}

		public IList<long> Find(long transactionNumber, int numberOfFreePages)
		{
			var buffer = GetBufferForTransaction(transactionNumber);

			var result = buffer.GetContinuousRangeOfFreePages(numberOfFreePages);

			if (result != null)
			{
				foreach (var freePageNumber in result)
				{
					buffer.Pages[freePageNumber] = false; // mark returned pages as busy
				}
			}

			return result;
		}

		public void Dispose()
		{
			bufferProviders[0].Dispose();
			bufferProviders[1].Dispose();
		}
	}
}