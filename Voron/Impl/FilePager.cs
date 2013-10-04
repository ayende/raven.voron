using System;
using System.IO;
using System.IO.MemoryMappedFiles;
using System.Runtime.InteropServices;
using System.Threading;
using Voron.Trees;

namespace Voron.Impl
{
	public unsafe class FilePager : AbstractPager
	{
		private readonly FlushMode _flushMode;
		private readonly FileStream _fileStream;
		private IntPtr _fileHandle;

		public FilePager(string file, FlushMode flushMode = FlushMode.Full)
		{
			_flushMode = flushMode;
			var fileInfo = new FileInfo(file);
			var noData = fileInfo.Exists == false || fileInfo.Length == 0;
			_fileStream = fileInfo.Open(FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.Read);

			_fileHandle = _fileStream.SafeFileHandle.DangerousGetHandle();

			if (noData)
			{
				NumberOfAllocatedPages = 0;
			}
			else
			{
                NumberOfAllocatedPages = fileInfo.Length / PageSize;
				PagerState.Release();
				PagerState = CreateNewPagerState();
			}
		}

		public override byte* AcquirePagePointer(long pageNumber)
		{
			return PagerState.Base + (pageNumber*PageSize);
		}

		public override void AllocateMorePages(Transaction tx, long newLength)
		{
			if (newLength < _fileStream.Length)
				throw new ArgumentException("Cannot set the legnth to less than the current length");

		    if (newLength == _fileStream.Length)
		        return;

			// need to allocate memory again
			_fileStream.SetLength(newLength);

			int lo = (int)(newLength & 0xffffffff);
			int hi = (int)(newLength >> 32);

			//UnmanagedFileAccess.SetFilePointer(_fileHandle, lo, out hi, UnmanagedFileAccess.EMoveMethod.Begin);
			//UnmanagedFileAccess.SetEndOfFile(_fileHandle);

			PagerState.Release(); // when the last transaction using this is over, will dispose it
			PagerState newPager = CreateNewPagerState();

			if (tx != null) // we only pass null during startup, and we don't need it there
			{
				newPager.AddRef(); // one for the current transaction
				tx.AddPagerState(newPager);
			}

			PagerState = newPager;
			NumberOfAllocatedPages = newPager.Accessor.Capacity / PageSize;
		}

		private PagerState CreateNewPagerState()
		{
			var mmf = MemoryMappedFile.CreateFromFile(_fileStream, Guid.NewGuid().ToString(), _fileStream.Length,
													  MemoryMappedFileAccess.Read, null, HandleInheritability.None, true);
			
			MemoryMappedViewAccessor accessor;
		    try
		    {
			    accessor = mmf.CreateViewAccessor(0, _fileStream.Length, MemoryMappedFileAccess.Read);
		    }
		    catch (Exception)
		    {
                mmf.Dispose();
		        throw;
		    }
		    byte* p = null;
			accessor.SafeMemoryMappedViewHandle.AcquirePointer(ref p);

			var newPager = new PagerState
				 {
					 Accessor = accessor,
					 File = mmf,
					 Base = p
				 };
			newPager.AddRef(); // one for the pager
			return newPager;
		}

		public override void Sync()
		{
			if (_flushMode == FlushMode.Full)
				_fileStream.Flush(true);
		}

	    public override void Write(Page page)
	    {
			uint written;
			
			var position = page.PageNumber * PageSize;
		    var nativeOverlapped = new NativeOverlapped()
			    {
				    OffsetLow = (int) (position & 0xffffffff),
				    OffsetHigh = (int) (position >> 16 >> 16)
			    };

		    var toWrite = page.IsOverflow ? (uint) page.OverflowSize : (uint) PageSize;


		    if (UnmanagedFileAccess.WriteFile(_fileHandle, new IntPtr(page.Base), toWrite, out written, ref nativeOverlapped) == false)
		    {
			    var win32Error = Marshal.GetLastWin32Error();
			    throw new IOException("Writing to file failed. Error code: " + win32Error);
		    }
	    }

	    public override void Dispose()
		{
            base.Dispose();
			if (PagerState != null)
			{
				PagerState.Release();
				PagerState = null;
			}
			_fileStream.Dispose();
		}
	}
}
