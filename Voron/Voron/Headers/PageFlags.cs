using System;

namespace Voron.Headers
{
    [Flags]
    public enum PageFlags : byte
    {
        None = 0,
        Overflow = 1
    }
}