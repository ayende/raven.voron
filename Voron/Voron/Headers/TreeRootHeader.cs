using System.Runtime.InteropServices;

namespace Voron.Headers
{
    [StructLayout(LayoutKind.Explicit, Pack = 1)]
    public struct TreeRootHeader
    {
        [FieldOffset(0)]
        public long RootPageNumber;
        [FieldOffset(8)]
        public long EntriesCount;
        [FieldOffset(16)]
        public long PageCount;
        [FieldOffset(24)]
        public int Depth;
    }
}