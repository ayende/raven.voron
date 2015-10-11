using System;
using System.Runtime.InteropServices;

namespace Voron.Platform.Posix
{
    [StructLayout(System.Runtime.InteropServices.LayoutKind.Sequential)]
    public struct sysinfo_t
    {
        public System.UIntPtr  uptime;             /* Seconds since boot */
        [System.Runtime.InteropServices.MarshalAs(System.Runtime.InteropServices.UnmanagedType.ByValArray, SizeConst=3)]
        public System.UIntPtr [] loads;  /* 1, 5, and 15 minute load averages */
        public System.UIntPtr totalram;  /* Total usable main memory size */

        public System.UIntPtr freeram;   /* Available memory size */
        public ulong AvailableRam {
            get { return (ulong)freeram; }
            set { freeram = new UIntPtr (value); }
        }

        public System.UIntPtr sharedram; /* Amount of shared memory */
        public System.UIntPtr bufferram; /* Memory used by buffers */
        public System.UIntPtr totalswap; /* Total swap space size */
        public System.UIntPtr freeswap;  /* swap space still available */
        public ushort procs;    /* Number of current processes */
        [System.Runtime.InteropServices.MarshalAs(System.Runtime.InteropServices.UnmanagedType.ByValArray, SizeConst=22)]
        public char[] _f; /* Pads structure to 64 bytes */
    }
}