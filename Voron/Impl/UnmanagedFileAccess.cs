// -----------------------------------------------------------------------
//  <copyright file="UnmanagedFileAccess.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.IO;
using System.Runtime.InteropServices;
using System.Threading;
using Microsoft.Win32.SafeHandles;

namespace Voron.Impl
{
	public static class UnmanagedFileAccess
	{
		[DllImport("kernel32.dll")]
		public static extern bool WriteFile(
			IntPtr hFile,
			IntPtr lpBuffer,
			uint nNumberOfBytesToWrite,
			out uint lpNumberOfBytesWritten,
			[In] ref NativeOverlapped lpOverlapped);

		//[DllImport("kernel32.dll", SetLastError = true)]
		//public static extern IntPtr CreateFile(
		//	string lpFileName, uint dwDesiredAccess,
		//	uint dwShareMode, IntPtr lpSecurityAttributes,
		//	uint dwCreationDisposition,
		//	uint dwFlagsAndAttributes, IntPtr hTemplateFile);
		[DllImport("kernel32.dll", CharSet = CharSet.Auto, SetLastError = true)]
		public static extern SafeFileHandle CreateFile(
			 [MarshalAs(UnmanagedType.LPTStr)] string filename,
			 [MarshalAs(UnmanagedType.U4)] FileAccess access,
			 [MarshalAs(UnmanagedType.U4)] FileShare share,
			 IntPtr securityAttributes, // optional SECURITY_ATTRIBUTES struct or IntPtr.Zero
			 [MarshalAs(UnmanagedType.U4)] FileMode creationDisposition,
			 [MarshalAs(UnmanagedType.U4)] FileAttributes flagsAndAttributes,
			 IntPtr templateFile);

		[DllImport("kernel32.dll", SetLastError = true)]
		public static extern bool CloseHandle(IntPtr hObject);

		[DllImport("kernel32.dll", SetLastError = true)]
		public static extern bool SetEndOfFile(SafeFileHandle hFile);

		[DllImport("Kernel32.dll", SetLastError = true, CharSet = CharSet.Auto)]
		public static extern uint SetFilePointer(
			[In] SafeFileHandle hFile,
			[In] int lDistanceToMove,
			[Out] out int lpDistanceToMoveHigh,
			[In] EMoveMethod dwMoveMethod);


		public enum EMoveMethod : uint
		{
			Begin = 0,
			Current = 1,
			End = 2
		}

		public const short FILE_ATTRIBUTE_NORMAL = 0x80;
		public const short INVALID_HANDLE_VALUE = -1;
		public const uint GENERIC_READ = 0x80000000;
		public const uint GENERIC_WRITE = 0x40000000;
		public const uint CREATE_NEW = 1;
		public const uint CREATE_ALWAYS = 2;
		public const uint OPEN_EXISTING = 3;   
	}
}