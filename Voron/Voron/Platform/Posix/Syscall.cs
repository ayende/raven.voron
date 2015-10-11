using System;
using System.Runtime.InteropServices;

namespace Voron.Platform.Posix
{
    // Take from https://github.com/mono/mono/blob/master/mcs/class/Mono.Posix/Mono.Unix.Native/Syscall.cs
    // Used this way to avoid taking a hard dependency on the Mono.Posix.dll
	public static class Syscall
	{
        internal const string LIBC = "libc";
        internal const string MPH = "MonoPosixHelper";

		[DllImport(LIBC, SetLastError = true)]
		public static extern int sysinfo(ref sysinfo_t info);

		[DllImport(LIBC, SetLastError = true)]
		public static extern int mkdir ([MarshalAs (UnmanagedType.LPStr)] string filename, [MarshalAs (UnmanagedType.U4)] uint mode);

        [DllImport(LIBC, SetLastError = true)]
        public static extern int close(int fd);
        // pread(2)
        //    ssize_t pread(int fd, void *buf, size_t count, off_t offset);
        [DllImport(MPH, SetLastError = true,
                EntryPoint = "Mono_Posix_Syscall_pread")]
        public static extern long pread(int fd, IntPtr buf, ulong count, long offset);

        public static unsafe long pread(int fd, void* buf, ulong count, long offset)
        {
            return pread(fd, (IntPtr)buf, count, offset);
        }

        // posix_fallocate(P)
        //    int posix_fallocate(int fd, off_t offset, size_t len);
        [DllImport(MPH, SetLastError = true,
                EntryPoint = "Mono_Posix_Syscall_posix_fallocate")]
        public static extern int posix_fallocate(int fd, long offset, ulong len);

        [DllImport(MPH, SetLastError = true,
                EntryPoint = "Mono_Posix_Syscall_msync")]
        public static extern int msync(IntPtr start, ulong len, MsyncFlags flags);


        [DllImport(MPH, SetLastError = true,
                EntryPoint = "Mono_Posix_Syscall_mmap")]
        public static extern IntPtr mmap(IntPtr start, ulong length,
                MmapProts prot, MmapFlags flags, int fd, long offset);

        [DllImport(MPH, SetLastError = true,
                EntryPoint = "Mono_Posix_Syscall_munmap")]
        public static extern int munmap(IntPtr start, ulong length);


        // getpid(2)
        //    pid_t getpid(void);
        [DllImport(LIBC, SetLastError = true)]
        public static extern int getpid();

        [DllImport(LIBC, SetLastError = true)]
        private static extern int unlink(
				IntPtr pathname);

	    public static  int unlink(string pathname)
	    {
            IntPtr pathNamePtr = Marshal.StringToHGlobalAuto(pathname);
            try
            {
                return unlink(pathNamePtr);
            }
            finally
            {
                Marshal.FreeHGlobal(pathNamePtr);
            }
	    }
        // open(2)
        //    int open(const char *pathname, int flags, mode_t mode);
        [DllImport(MPH, SetLastError = true,
                EntryPoint = "Mono_Posix_Syscall_open_mode")]
        private static extern int open(
				IntPtr pathname, OpenFlags flags, FilePermissions mode);

	    public static int open(
	        string pathname, OpenFlags flags, FilePermissions mode)
	    {
	        IntPtr pathNamePtr = Marshal.StringToHGlobalAuto(pathname);
	        try
	        {
	            return open(pathNamePtr, flags, mode);
	        }
	        finally
	        {
	            Marshal.FreeHGlobal(pathNamePtr);
	        }
	    }


	    [DllImport(LIBC, SetLastError = true)]
        public static extern int fsync(int fd);


        // read(2)
        //    ssize_t read(int fd, void *buf, size_t count);
        [DllImport(MPH, SetLastError = true,
                EntryPoint = "Mono_Posix_Syscall_read")]
        public static extern long read(int fd, IntPtr buf, ulong count);

        public static unsafe long read(int fd, void* buf, ulong count)
        {
            return read(fd, (IntPtr)buf, count);
        }

        // pwritev(2)
        //    ssize_t pwritev(int fd, const struct iovec *iov, int iovcnt, off_t offset);
        [DllImport(MPH, SetLastError = true,
                EntryPoint = "Mono_Posix_Syscall_pwritev")]
        private static extern long sys_pwritev(int fd, Iovec[] iov, int iovcnt, long offset);

        public static long pwritev(int fd, Iovec[] iov, long offset)
        {
            return sys_pwritev(fd, iov, iov.Length, offset);
        }


        // pwrite(2)
        //    ssize_t pwrite(int fd, const void *buf, size_t count, off_t offset);
        [DllImport(MPH, SetLastError = true,
                EntryPoint = "Mono_Posix_Syscall_pwrite")]
        public static extern long pwrite(int fd, IntPtr buf, ulong count, long offset);

        public static unsafe long pwrite(int fd, void* buf, ulong count, long offset)
        {
            return pwrite(fd, (IntPtr)buf, count, offset);
        }


        // write(2)
        //    ssize_t write(int fd, const void *buf, size_t count);
        [DllImport(MPH, SetLastError = true,
                EntryPoint = "Mono_Posix_Syscall_write")]
        public static extern long write(int fd, IntPtr buf, ulong count);

        public static unsafe long write(int fd, void* buf, ulong count)
        {
            return write(fd, (IntPtr)buf, count);
        }


        [DllImport(MPH, SetLastError = true,
                EntryPoint = "Mono_Posix_Syscall_sysconf")]
        public static extern long sysconf(SysconfName name, Errno defaultError);

        public static long sysconf(SysconfName name)
        {
            return sysconf(name, (Errno)0);
        }

        [DllImport(MPH, SetLastError = true,
                EntryPoint = "Mono_Posix_Syscall_fstat")]
        public static extern int fstat(int filedes, out Stat buf);

	}

    // Use manually written To/From methods to handle fields st_atime_nsec etc.

    // mode_t
}
