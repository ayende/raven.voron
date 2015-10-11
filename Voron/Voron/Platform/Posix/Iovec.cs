using System;

namespace Voron.Platform.Posix
{
    public struct Iovec
    {
        public IntPtr iov_base; // Starting address
        [CLSCompliant(false)]
        public ulong iov_len;  // Number of bytes to transfer
    }
}