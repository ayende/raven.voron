using System;

namespace Voron.Headers
{
    [Flags]
    public enum TransactionMarker : byte
    {
        None = 0x0,
        Commit = 0x4,
        Merged = 0x8
    }
}