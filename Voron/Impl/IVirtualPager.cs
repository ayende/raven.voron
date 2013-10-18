using System;
using Voron.Trees;

namespace Voron.Impl
{
    public unsafe interface IVirtualPager : IDisposable
    {
		PagerState PagerState { get; }

		byte* AcquirePagePointer(long pageNumber);
        Page Read(long pageNumber);
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

		bool ShouldGoToOverflowPage(int len);

		int GetNumberOfOverflowPages(Transaction tx, int overflowSize);

        void EnsureContinuous(Transaction tx, long requestedPageNumber, int pageCount);
        int Write(Transaction tx, Page page);
	    Page GetWritable(long pageNumber);
	}
}