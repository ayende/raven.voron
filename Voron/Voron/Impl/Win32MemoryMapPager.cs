using System.Collections.Generic;
using Voron.Pagers;

namespace Voron.Impl
{
    public class Win32MemoryMapPager : IVirtualPager
    {
        public Win32MemoryMapPager(string filename)
        {
            throw new System.NotImplementedException();
        }

        public void Dispose()
        {
            throw new System.NotImplementedException();
        }

        public unsafe PagerState PagerState { get; private set; }
        public unsafe bool Disposed { get; private set; }
        public unsafe long NumberOfAllocatedPages { get; private set; }
        public unsafe int PageMinSpace { get; private set; }
        public unsafe bool DeleteOnClose { get; set; }

        public unsafe void Sync()
        {
            throw new System.NotImplementedException();
        }

        public unsafe PagerState TransactionBegan()
        {
            throw new System.NotImplementedException();
        }

        public unsafe bool ShouldGoToOverflowPage(int len)
        {
            throw new System.NotImplementedException();
        }

        public unsafe int GetNumberOfOverflowPages(int overflowSize)
        {
            throw new System.NotImplementedException();
        }

        public unsafe bool WillRequireExtension(long requestedPageNumber, int numberOfPages)
        {
            throw new System.NotImplementedException();
        }

        public unsafe PagerState EnsureContinuous(long requestedPageNumber, int numberOfPages)
        {
            throw new System.NotImplementedException();
        }

        public unsafe byte* AcquirePagePointer(long pageNumber, PagerState pagerState = null)
        {
            throw new System.NotImplementedException();
        }

        public unsafe PagerState AllocateMorePages(long newLength)
        {
            throw new System.NotImplementedException();
        }

        public unsafe int WriteDirect(byte* start, long pageNum, int pagesToWrite)
        {
            throw new System.NotImplementedException();
        }

        public unsafe void MaybePrefetchMemory(List<Page> sortedPages)
        {
            throw new System.NotImplementedException();
        }

        public unsafe void TryPrefetchingWholeFile()
        {
            throw new System.NotImplementedException();
        }

        public unsafe void ReleaseAllocationInfo(byte* baseAddress, long size)
        {
            throw new System.NotImplementedException();
        }
    }
}