// -----------------------------------------------------------------------
//  <copyright file="NativeFileMethods.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.ComponentModel;
using System.IO;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using Microsoft.Win32.SafeHandles;
using Voron.Exceptions;

namespace Voron.Platform.Win32
{
	public static unsafe class Win32NativeFileMethods
	{
        public const int ErrorIOPending = 997;
        public const int ErrorSuccess = 0;
        public const int ErrorHandleEof = 38;


	    [StructLayout(LayoutKind.Explicit, Size = 8)]
	    public struct FileSegmentElement
	    {
	        [FieldOffset(0)]
	        public IntPtr Buffer;
	        [FieldOffset(0)]
	        public UInt64 Alignment;
	    }


	    [DllImport("kernel32.dll", SetLastError = true)]
	    [return: MarshalAs(UnmanagedType.Bool)]
	    public static extern bool WriteFileGather(
	        SafeFileHandle hFile,
	        FileSegmentElement* aSegmentArray,
	        uint nNumberOfBytesToWrite,
	        IntPtr lpReserved,
	        NativeOverlapped* lpOverlapped);

	    [DllImport("kernel32.dll", SetLastError = true)]
	    public static extern bool GetOverlappedResult(SafeFileHandle hFile,
	        NativeOverlapped* lpOverlapped,
	        out uint lpNumberOfBytesTransferred, bool bWait);

	    [DllImport("kernel32.dll", SetLastError = true)]
        public static extern bool SetFilePointerEx(SafeFileHandle hFile, long liDistanceToMove,
           IntPtr lpNewFilePointer, Win32NativeFileMoveMethod dwMoveMethod);

        public delegate void WriteFileCompletionDelegate(UInt32 dwErrorCode, UInt32 dwNumberOfBytesTransfered, NativeOverlapped* lpOverlapped);

        [DllImport("kernel32.dll", SetLastError = true)]
        public static extern bool WriteFileEx(SafeFileHandle hFile, byte* lpBuffer,
           uint nNumberOfBytesToWrite, NativeOverlapped* lpOverlapped,
           WriteFileCompletionDelegate lpCompletionRoutine);

		[DllImport("kernel32.dll", SetLastError = true)]
		public static extern bool WriteFile(SafeFileHandle hFile, byte* lpBuffer, int nNumberOfBytesToWrite,
		                                    out int lpNumberOfBytesWritten, NativeOverlapped* lpOverlapped);



		[DllImport(@"kernel32.dll", SetLastError = true)]
		public static extern bool ReadFile(
			SafeFileHandle hFile,
			byte* pBuffer,
			int numBytesToRead,
			out int pNumberOfBytesRead,
			NativeOverlapped* lpOverlapped
			);

		[DllImport("kernel32.dll", SetLastError = true, CharSet = CharSet.Auto)]
		public static extern SafeFileHandle CreateFile(string lpFileName,
		                                               Win32NativeFileAccess dwDesiredAccess, Win32NativeFileShare dwShareMode,
		                                               IntPtr lpSecurityAttributes,
		                                               Win32NativeFileCreationDisposition dwCreationDisposition,
		                                               Win32NativeFileAttributes dwFlagsAndAttributes, IntPtr hTemplateFile);

		[DllImport("kernel32.dll", SetLastError = true)]
		public static extern bool CloseHandle(IntPtr hObject);

		[DllImport("kernel32.dll", SetLastError = true)]
		private static extern bool SetEndOfFile(SafeFileHandle hFile);

		[DllImport("kernel32.dll", SetLastError = true)]
		public static extern bool FlushFileBuffers(SafeFileHandle hFile);

		[DllImport("kernel32.dll", EntryPoint = "GetFinalPathNameByHandleW", CharSet = CharSet.Unicode, SetLastError = true)]
		public static extern int GetFinalPathNameByHandle(SafeFileHandle handle, [In, Out] StringBuilder path, int bufLen, int flags);
		
		public static void SetFileLength(SafeFileHandle fileHandle, long length)
		{
		    if (SetFilePointerEx(fileHandle, length, IntPtr.Zero, Win32NativeFileMoveMethod.Begin) == false)
			{
                throw new Win32Exception(Marshal.GetLastWin32Error());
			}

			if (SetEndOfFile(fileHandle) == false)
			{
				var lastError = Marshal.GetLastWin32Error();

				if (lastError == (int) Win32NativeFileErrors.ERROR_DISK_FULL)
				{
					var filePath = new StringBuilder(256);

					while (GetFinalPathNameByHandle(fileHandle, filePath, filePath.Capacity, 0) > filePath.Capacity && 
						filePath.Capacity < 32767) // max unicode path length
					{
					    filePath.EnsureCapacity(filePath.Capacity*2);
					}

					filePath = filePath.Replace(@"\\?\", String.Empty); // remove extended-length path prefix

					var fullFilePath = filePath.ToString();
					var driveLetter = Path.GetPathRoot(fullFilePath);
					var driveInfo = new DriveInfo(driveLetter);

					throw new DiskFullException(driveInfo, fullFilePath, length);
				}

				var exception = new Win32Exception(lastError);

				if (lastError == (int) Win32NativeFileErrors.ERROR_NOT_READY || lastError == (int) Win32NativeFileErrors.ERROR_FILE_NOT_FOUND)
					throw new VoronUnrecoverableErrorException("Could not set the file size because it is inaccessible", exception);

				throw exception;
			}
		}
	}
}