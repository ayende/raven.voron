using System.Runtime.CompilerServices;
using Voron.Headers;

namespace Voron.Pagers
{
    public unsafe class Page
    {
        private readonly PageHeader* _header;

        public Page(byte* ptr)
        {
            _header = (PageHeader*)ptr;
        }

        public long PageNumber
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return _header->PageNumber; }
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set { _header->PageNumber = value; }
        }

        public bool IsOverflow
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return (_header->Flags & PageFlags.Overflow) == PageFlags.Overflow; }
        }

        public int OverflowSize
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return _header->OverflowSize; }
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set { _header->OverflowSize = value; }
        }

    }
}