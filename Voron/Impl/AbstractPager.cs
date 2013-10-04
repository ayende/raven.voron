using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using Voron.Impl.FileHeaders;
using Voron.Impl.FreeSpace;
using Voron.Trees;

namespace Voron.Impl
{
    public unsafe abstract class AbstractPager : IVirtualPager
    {
        protected const int MinIncreaseSize = 1 * 1024 * 1024;

        private long _increaseSize = MinIncreaseSize;
        private DateTime _lastIncrease;
        private IntPtr _tempPage;
        public PagerState PagerState { get; protected set; }

        protected AbstractPager()
        {
            MaxNodeSize = (PageSize - Constants.PageHeaderSize) / Constants.MinKeysInPage;
            PageMaxSpace = PageSize - Constants.PageHeaderSize;
            PageMinSpace = (int)(PageMaxSpace * 0.33);
            PagerState = new PagerState();
            _tempPage = Marshal.AllocHGlobal(PageSize);
           PagerState.AddRef();
        }

        public int PageMaxSpace { get; private set; }
        public int MaxNodeSize { get; private set; }
        public int PageMinSpace { get; private set; }

        public int PageSize
        {
            get { return 4096; }
        }

        public long NumberOfAllocatedPages { get; protected set; }

        public Page Get(Transaction tx, long pageNumber, bool errorOnChange = false)
        {
            if (pageNumber + 1 > NumberOfAllocatedPages && errorOnChange)
            {
                throw new InvalidOperationException("Cannot increase size of the pager when errorOnChange is set to true");
            }
            EnsureContinuous(tx, pageNumber, 1);
            return Get(pageNumber);
        }

	    public abstract byte* AcquirePagePointer(long pageNumber);

        protected Page Get(long n)
        {
			return new Page(AcquirePagePointer(n), PageMaxSpace);
        }

        public abstract void Flush(List<long> sortedPagesToFlush);
        public abstract void Flush(long headerPageId);
        public abstract void Sync();

        public virtual PagerState TransactionBegan()
        {
            var state = PagerState;
            state.AddRef();
            return state;
        }

        public virtual void EnsureContinuous(Transaction tx, long requestedPageNumber, int pageCount)
        {
            if (requestedPageNumber + pageCount <= NumberOfAllocatedPages)
                return;

            // this ensure that if we want to get a range that is more than the current expansion
            // we will increase as much as needed in one shot
            var minRequested = (requestedPageNumber + pageCount) * PageSize;
            var allocationSize = NumberOfAllocatedPages * PageSize;
            while (minRequested > allocationSize)
            {
                allocationSize = GetNewLength(allocationSize);
            }

	        var numberOfPagesAfterAllocation = allocationSize/PageSize;

			if (tx != null && numberOfPagesAfterAllocation > tx.Environment.FreeSpaceHandling.MaxNumberOfPages)
			{
				// Need to take into account size of free space allocation, if we need to re-allocate free space
				allocationSize += UnmanagedBits.CalculateSizeInBytesForAllocation(2 * numberOfPagesAfterAllocation, PageSize);
			}
			
            AllocateMorePages(tx, allocationSize);

			if (tx != null)
			{
				EnsureFreeSpaceTrackingHasEnoughSpace(tx, pageCount);
			}
        }

        public abstract unsafe void Write(Page page);

        public void EnsureFreeSpaceTrackingHasEnoughSpace(Transaction tx, int pageCount)
		{
			if (tx.Environment.FreeSpaceHandling.MaxNumberOfPages >= NumberOfAllocatedPages)
			{
				if (NumberOfAllocatedPages > tx.Environment.FreeSpaceHandling.NumberOfTrackedPages)
				{
					tx.Environment.FreeSpaceHandling.TrackMorePages(NumberOfAllocatedPages);
				}

				// even if we don't need to allocate more pages for free space handling and move the buffers
				// we have to update pointers of buffers because when we allocate new pages we change the PagerState
				// used by an acquisition of  page pointers
				tx.Environment.FreeSpaceHandling.UpdateBufferPointers();

				return;
			}
			//TODO arek
			//var requiredSize = UnmanagedBits.CalculateSizeInBytesForAllocation(2 * NumberOfAllocatedPages, PageSize);
			//var requiredPages = (long)Math.Ceiling((float)requiredSize / PageSize);
			//// we always allocate twice as much as we actually need, because we don't 

			//// we request twice because it would likely be easier to find two smaller pieces than one big piece
			//var firstBufferPageStart = tx.FreeSpaceBuffer.Find(requiredPages);
			//var secondBufferPageStart = tx.FreeSpaceBuffer.Find(requiredPages);

			//// this is a bit of a hack, because we modify the NextPageNumber just before
			//// the tx is going to modify it, too.
			//// However, this is currently safe because the tx will just add to its current value
			//// so we can do that. However, note that we need to skip the range that it is _currently_
			//// allocating
			//if (firstBufferPageStart == -1)
			//{
			//	firstBufferPageStart = tx.NextPageNumber;
			//	tx.NextPageNumber += requiredPages;
			//}

			//if (secondBufferPageStart == -1)
			//{
			//	secondBufferPageStart = tx.NextPageNumber;
			//	tx.NextPageNumber += requiredPages;
			//}

			//if (pageCount + tx.NextPageNumber >= NumberOfAllocatedPages)
			//	throw new InvalidOperationException(
			//		"BUG, cannot find space for free space during file growth because the required size was too big even after the re-growth");

			//tx.Environment.FreeSpaceHandling.MoveTo(firstBufferPageStart, secondBufferPageStart, NumberOfAllocatedPages,
			//										requiredPages, PageSize);

			//// after moving buffers to new pages make sure that they will be flushed
			//var buffersPages = tx.Environment.FreeSpaceHandling.Info.GetBuffersPages();
			//buffersPages.Sort();

			//Flush(buffersPages);

			//// and both file headers will contain modified free space handling information
			//var fileHeader = (FileHeader*)Get(tx, 0).Base + Constants.PageHeaderSize;
			//tx.Environment.FreeSpaceHandling.CopyStateTo(&fileHeader->FreeSpace);

			//fileHeader = (FileHeader*)Get(tx, 1).Base + Constants.PageHeaderSize;
			//tx.Environment.FreeSpaceHandling.CopyStateTo(&fileHeader->FreeSpace);

			//Flush(new List<long> {0, 1});

			//Sync();
		}

	    public virtual void Dispose()
        {
            if (_tempPage != IntPtr.Zero)
            {
                Marshal.FreeHGlobal(_tempPage);
                _tempPage = IntPtr.Zero;
            }
        }

        public abstract void AllocateMorePages(Transaction tx, long newLength);

        public Page TempPage
        {
            get
            {
                return new Page((byte*)_tempPage.ToPointer(), PageMaxSpace)
                {
                    Upper = (ushort)PageSize,
                    Lower = (ushort)Constants.PageHeaderSize,
                    Flags = 0,
                };
            }
        }

        private long GetNewLength(long current)
        {
            DateTime now = DateTime.UtcNow;
            TimeSpan timeSinceLastIncrease = (now - _lastIncrease);
            if (timeSinceLastIncrease.TotalSeconds < 30)
            {
	            _increaseSize = Math.Min(_increaseSize*2, current + current/4);
            }
            else if (timeSinceLastIncrease.TotalMinutes > 2)
            {
                _increaseSize = Math.Max(MinIncreaseSize, _increaseSize / 2);
            }
            _lastIncrease = now;
            // At any rate, we won't do an increase by over 25% of current size, to prevent huge empty spaces
            // and the first size we allocate is 256 pages (1MB)
            // 
            // The reasoning behind this is that we want to make sure that we increase in size very slowly at first
            // because users tend to be sensitive to a lot of "wasted" space. 
            // We also consider the fact that small increases in small files would probably result in cheaper costs, and as
            // the file size increases, we will reserve more & more from the OS.
            // This also plays avoids "I added 300 records and the file size is 64MB" problems that occur when we are too
            // eager to reserve space
            current = Math.Max(current, 256 * PageSize);
            var actualIncrease = Math.Min(_increaseSize, current / 4);

	        return current + actualIncrease;
        }
    }
}
