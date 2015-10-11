using System.Runtime.InteropServices;

namespace Voron.Headers
{
    [StructLayout(LayoutKind.Explicit, Pack = 1)]
    public struct PageHeader
    {
        [FieldOffset(0)]
        public long PageNumber;

        [FieldOffset(8)]
        public int OverflowSize;

        [FieldOffset(12)]
        public PageFlags Flags;
    }
}