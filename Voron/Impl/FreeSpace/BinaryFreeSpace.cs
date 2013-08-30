// -----------------------------------------------------------------------
//  <copyright file="FreePagesRepository.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Diagnostics;
using System.IO;

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


		private void CreateBitBuffers(long numberOfPages)
		{
			NumberOfTrackedPages = numberOfPages;

			for (int i = 0; i < 2; i++)
			{
				var size = UnmanagedBits.CalculateSizeInBytesForAllocation(numberOfPages);

				bufferProviders[i].IncreaseSize(size);

				bufferProviders[i].Release();

				Buffers[i] = new FreeSpaceBuffer(bufferProviders[i].GetBufferPointer(), numberOfPages);
			}
		}


		public void Dispose()
		{
			bufferProviders[0].Dispose();
			bufferProviders[1].Dispose();
		}
	}
}