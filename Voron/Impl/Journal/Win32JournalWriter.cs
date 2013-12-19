namespace Voron.Impl.Journal
{
	using System;
	using System.ComponentModel;
	using System.IO;
	using System.Runtime.InteropServices;
	using System.Threading;
	using System.Threading.Tasks;

	using Microsoft.Win32.SafeHandles;

	using Voron.Impl.Paging;
	using Voron.Util;

	public unsafe class Win32FileJournalWriter: IJournalWriter
	{
		private readonly string _filename;
		private readonly SafeFileHandle _handle;
		private SafeFileHandle _readHandle;

		[DllImport("kernel32.dll")]
		static extern bool WriteFileGather(
			SafeFileHandle hFile,
			FileSegmentElement* aSegmentArray,
			uint nNumberOfBytesToWrite,
			IntPtr lpReserved,
			NativeOverlapped* lpOverlapped);


		[StructLayout(LayoutKind.Explicit, Size = 8)]
		public struct FileSegmentElement
		{
			[FieldOffset(0)]
			public byte* Buffer;
			[FieldOffset(0)]
			public UInt64 Alignment;
		}

		public Win32FileJournalWriter(string filename, long journalSize)
		{
			_filename = filename;
			_handle = NativeFileMethods.CreateFile(filename,
				NativeFileAccess.GenericWrite, NativeFileShare.Read, IntPtr.Zero,
				NativeFileCreationDisposition.OpenAlways,
				NativeFileAttributes.Write_Through | NativeFileAttributes.NoBuffering | NativeFileAttributes.Overlapped, IntPtr.Zero);

			if (_handle.IsInvalid)
				throw new Win32Exception();

			NativeFileMethods.SetFileLength(_handle, journalSize);

			NumberOfAllocatedPages = journalSize/AbstractPager.PageSize;

			if (ThreadPool.BindHandle(_handle) == false)
				throw new InvalidOperationException("Could not bind the handle to the thread pool");
		}

		private const int ErrorIOPending = 997;
		private const int ErrorSuccess = 0;
		private const int ErrorOperationAborted = 995;
		private const int ErrorHandleEof = 38;

		public void WriteGather(long position, byte*[] pages)
		{
			if (Disposed)
				throw new ObjectDisposedException("Win32JournalWriter");

		    using (var mre = new ManualResetEvent(false))
		    {
		        var allocHGlobal = Marshal.AllocHGlobal(sizeof (FileSegmentElement)*(pages.Length + 1));
		        uint resultCode = 0;
		        var o = new Overlapped((int) (position & 0xffffffff), (int) (position >> 32),
		            mre.SafeWaitHandle.DangerousGetHandle(), null);
		        var array = (FileSegmentElement*)allocHGlobal.ToPointer();
		        for (int i = 0; i < pages.Length; i++)
		        {
		            array[i].Buffer = pages[i];
		        }
		        array[pages.Length].Buffer = null; // null terminating

		        WriteFileGather(_handle, array, (uint)pages.Length * 4096, IntPtr.Zero, o.Pack((code, bytes, overlap) =>
		        {
		            resultCode = code;
		            Overlapped.Free(overlap);
		            if (allocHGlobal != IntPtr.Zero)
		                Marshal.FreeHGlobal(allocHGlobal);
		        }, null));
		        var lastWin32Error = Marshal.GetLastWin32Error();
		        if(lastWin32Error != ErrorSuccess && lastWin32Error != ErrorIOPending)
		                throw new Win32Exception(lastWin32Error);

                mre.WaitOne();

		        if (resultCode != ErrorSuccess)
		            throw new Win32Exception((int)resultCode);
		    }
		}

		public long NumberOfAllocatedPages { get; private set; }
	    public bool DeleteOnClose { get; set; }

	    public IVirtualPager CreatePager()
		{
			return new Win32MemoryMapPager(_filename);
		}

	    public bool Read(long pageNumber, byte* buffer, int count)
	    {
		    if (_readHandle == null)
		    {
			    _readHandle = NativeFileMethods.CreateFile(_filename,
				    NativeFileAccess.GenericRead, 
					NativeFileShare.Write | NativeFileShare.Read | NativeFileShare.Delete, 
					IntPtr.Zero,
				    NativeFileCreationDisposition.OpenExisting, 
					NativeFileAttributes.Normal, 
					IntPtr.Zero);
		    }
			
	        var position = pageNumber*AbstractPager.PageSize;
	        var overlapped = new Overlapped((int) (position & 0xffffffff), (int) (position >> 32), IntPtr.Zero, null);
	        var nativeOverlapped = overlapped.Pack(null, null);
	        try
	        {
	            while (count >0)
	            {
                    int read;
		            if (NativeFileMethods.ReadFile(_readHandle, buffer, count, out read, nativeOverlapped) == false)
		            {
			            var lastWin32Error = Marshal.GetLastWin32Error();
			            if (lastWin32Error == ErrorHandleEof)
				            return false;
			            throw new Win32Exception(lastWin32Error);
		            }
	                count -= read;
	                buffer += read;
	            }
		        return true;
	        }
	        finally
	        {
	            Overlapped.Free(nativeOverlapped);
	        }
	    }

	    public void Dispose()
		{
			Disposed = true;
			GC.SuppressFinalize(this);
			if (_readHandle != null)
				_readHandle.Close();
			_handle.Close();
		    if (DeleteOnClose)
		    {
		        File.Delete(_filename);
		    }
		}

		public bool Disposed { get; private set; }

		~Win32FileJournalWriter()
		{
			_handle.Close();
		}
	}
}