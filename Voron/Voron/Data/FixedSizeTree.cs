using Voron.Headers;
using Voron.Transactions;

namespace Voron.Data
{
    public unsafe class FixedSizeTree
    {
        internal const int BranchEntrySize = sizeof(long) + sizeof(long);

        private readonly InternalTransaction _tx;
        private readonly byte _valSize;
        private readonly TreeRootHeader _header;
        private readonly int _entrySize;
        private readonly int _maxEmbeddedEntries;

        public static TreeRootHeader Create(InternalTransaction tx, byte valSize)
        {
            var pageHeader = tx.Allocate(1);
            PageHeader

        }
    }
}