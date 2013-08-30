// -----------------------------------------------------------------------
//  <copyright file="FreePagesRepository.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Diagnostics;
using System.IO;
using Voron.Util;

namespace Voron.Impl.FreeSpace
{
	public unsafe class BinaryFreeSpace : IDisposable
	{
		private readonly IBufferProvider[] bufferProviders;

		public BinaryFreeSpace(string name, long numberOfPages)
		{
			bufferProviders = new IBufferProvider[2];
			Buffers = new FreeSpaceBuffer[2];

			for (var i = 0; i < 2; i++)
			{
				bufferProviders[i] = new MemoryMapBufferProvider(string.Format("{0}-{1}", name, i));
			}

			CreateBitBuffers(numberOfPages);
		}

		public FreeSpaceBuffer[] Buffers { get; private set; }

		public long NumberOfTrackedPages { get; private set; }

		private void CreateBitBuffers(long numberOfPages)
		{
			NumberOfTrackedPages = numberOfPages;

			for (int i = 0; i < 2; i++)
			{
				var size = FreeSpaceBuffer.CalculateSizeInBytesForAllocation(numberOfPages);

				bufferProviders[i].IncreaseSize(size);

				bufferProviders[i].Release();

				Buffers[i] = new FreeSpaceBuffer(bufferProviders[i].GetBufferPointer(), numberOfPages);
			}
		}

		public FreeSpaceBuffer GetBufferForTransaction(long transactionNumber)
		{
			var indexOfBuffer = transactionNumber & 1;

			var current = Buffers[indexOfBuffer];
			var reference = Buffers[1 - indexOfBuffer];

			if (current.IsDirty)
			{
				if (reference.IsDirty)
					throw new InvalidDataException(
						"Both buffers are dirty. Valid state of the free pages buffer cannot be restored. Transaction number: " +
						transactionNumber); // should never happen

				Debug.Assert(current.AllBits.Size == reference.AllBits.Size);

				NativeMethods.memcpy((byte*) current.AllBits.Ptr, (byte*) reference.AllBits.Ptr,
				                     (int) UnmanagedBits.GetSizeInBytesFor(reference.AllBits.Size));
			}
			else
			{
				Debug.Assert(current.ModifiedPages.Size == reference.ModifiedPages.Size);

				for (int i = 0; i < reference.ModifiedPages.Size; i++)
				{
					if (reference.ModifiedPages[i])
					{
						current.FreePages[i] = reference.FreePages[i];
					}
				}
			}

			current.ModifiedPages.Clear();

			return current;
		}

		public void Dispose()
		{
			bufferProviders[0].Dispose();
			bufferProviders[1].Dispose();
		}
	}
}