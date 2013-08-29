// -----------------------------------------------------------------------
//  <copyright file="MemoryMapBufferProvider.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.IO;
using System.IO.MemoryMappedFiles;

namespace Voron.Impl
{
	public unsafe class MemoryMapBufferProvider : IBufferProvider
	{
		private readonly FileStream fileStream;
		private MemoryMappedViewAccessor accessor;
		private MemoryMappedFile mmf;

		public MemoryMapBufferProvider(string name)
		{
			var file = new FileInfo(name);
			fileStream = file.Open(FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.Read);
		}

		public void IncreaseSize(long size)
		{
			fileStream.SetLength(size);
		}

		public int* GetBufferPointer()
		{
			mmf = MemoryMappedFile.CreateFromFile(fileStream, Guid.NewGuid().ToString(), fileStream.Length,
													  MemoryMappedFileAccess.ReadWrite, null, HandleInheritability.None, true);
			accessor = mmf.CreateViewAccessor();

			byte* ptr = null;
			accessor.SafeMemoryMappedViewHandle.AcquirePointer(ref ptr);

			return (int *)ptr;
		}

		public void Release()
		{
			if (accessor != null)
			{
				accessor.SafeMemoryMappedViewHandle.ReleasePointer();
				accessor.Dispose();
			}

			if (mmf != null)
			{
				mmf.Dispose();
			}
		}

		public void Dispose()
		{
			Release();

			fileStream.Dispose();
		}
	}
}