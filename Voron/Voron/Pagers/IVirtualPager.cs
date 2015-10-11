using System;
using System.Collections.Generic;
using Voron.Impl;

namespace Voron.Pagers
{
    public unsafe interface IVirtualPager : IDisposable
    {
		PagerState PagerState { get; }
		bool Disposed { get; }
		long NumberOfAllocatedPages { get; }
		int PageMinSpace { get; }
	    bool DeleteOnClose { get; set; }
	    void Sync();
		PagerState TransactionBegan();
		bool ShouldGoToOverflowPage(int len);
		int GetNumberOfOverflowPages(int overflowSize);
	    bool WillRequireExtension(long requestedPageNumber, int numberOfPages);
        PagerState EnsureContinuous(long requestedPageNumber, int numberOfPages);
        byte* AcquirePagePointer(long pageNumber, PagerState pagerState = null);
        PagerState AllocateMorePages(long newLength);
        int WriteDirect(byte* start, long pageNum, int pagesToWrite);
        void MaybePrefetchMemory(List<Page> sortedPages);
        void TryPrefetchingWholeFile();
        void ReleaseAllocationInfo(byte* baseAddress, long size);
    }
}
