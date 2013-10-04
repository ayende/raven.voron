using System;
using System.IO;
using System.Threading;
using Voron.Impl;

namespace Voron.Tryout
{
    internal class Program
    {
        private static void Main(string[] args)
        {
	        var filePtr = UnmanagedFileAccess.CreateFile("file.txt", FileAccess.Write, FileShare.None, IntPtr.Zero,
	                                                     FileMode.OpenOrCreate, FileAttributes.Normal, IntPtr.Zero);

	        uint written;
			NativeOverlapped n = new NativeOverlapped();
	        n.OffsetLow = 1024 * 1024 * 2;
	        unsafe
	        {
		        var bytes = new byte[] {1, 2, 3};
		        fixed (byte* p = bytes)
		        {
			        UnmanagedFileAccess.WriteFile(filePtr.DangerousGetHandle(), new IntPtr(p), 3, out written, ref n);
		        }
	        }
	        
			filePtr.Close();
	        //UnmanagedFileAccess.CloseHandle(filePtr.DangerousGetHandle());
        }

		private static int Test(int i)
		{
			return i >> 5;
		}

		private static int PrevPowerOfTwo(int x)
		{
			if ((x & (x - 1)) == 0)
				return x;

			x--;
			x |= x >> 1;
			x |= x >> 2;
			x |= x >> 4;
			x |= x >> 8;
			x |= x >> 16;
			x++;

			return x >> 1;
		}
    }
}