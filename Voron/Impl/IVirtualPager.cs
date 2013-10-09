using System;
using Voron.Trees;

namespace Voron.Impl
{
    public unsafe interface IVirtualPager : IDisposable
    {
        PagerState PagerState { get; }

		byte* AcquirePagePointer(long pageNumber);
        Page Read(Transaction tx, long pageNumber);
		void AllocateMorePages(Transaction tx, long newLength);
	    void Flush(long startPage, long count);

        Page TempPage { get; }

        long NumberOfAllocatedPages { get; }
        int PageSize { get; }
        int MaxNodeSize { get; }
        int PageMaxSpace { get; }
        int PageMinSpace { get; }

	    void Sync();

        PagerState TransactionBegan();

        void EnsureContinuous(Transaction tx, long requestedPageNumber, int pageCount);
        int Write(Page page);
	    int Write(Page page, long writeToPage);
	    Page GetWritable(long pageNumber);
    }
}