using System;
using Voron.Headers;
using Voron.Pagers;

namespace Voron.Transactions
{
    public unsafe class InternalTransaction : IDisposable
    {
        private readonly IVirtualPager _dataPager;
        private readonly long _id;

        public InternalTransaction(StorageEnvironmentOptions options, long id)
        {
            _id = id;
            _dataPager = options.DataPager;
        }

        public void Commit()
        {
            
        }

        public void Rollback()
        {
            
        }

        public PageHeader* Modify(long pageNumber)
        {
            
        }

        public PageHeader* Allocate(int numberOfPages)
        {
            
        }

        public void FreePage(long pageNumber)
        {
            
        }
      
        public void Dispose()
        {
            
        }
    }
}