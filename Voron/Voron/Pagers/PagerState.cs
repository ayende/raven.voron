using System.Diagnostics;
using System.IO.MemoryMappedFiles;
using System.Threading;

namespace Voron.Pagers
{
    public unsafe class PagerState
    {
	    private readonly IVirtualPager _pager;

	    public bool DisposeFilesOnDispose = true;

	    public class AllocationInfo
	    {
		    public MemoryMappedFile MappedFile;
		    public byte* BaseAddress;
		    public long Size;
	    }

        public PagerState(IVirtualPager pager)
        {
	        _pager = pager;
		}

        private int _refs;

        public MemoryMappedFile[] Files;

		public AllocationInfo[] AllocationInfos;

        public byte* MapBase { get; set; }

        public bool Released;

        public void Release()
        {
            if (Interlocked.Decrement(ref _refs) != 0)
                return;
         
            ReleaseInternal();
        }

        private void ReleaseInternal()
        {
			if (AllocationInfos != null)
			{
				foreach (var allocationInfo in AllocationInfos)
					_pager.ReleaseAllocationInfo(allocationInfo.BaseAddress, allocationInfo.Size);
			}

			if (Files != null && DisposeFilesOnDispose)
            {
	            foreach (var file in Files)
					file.Dispose();

	            Files = null;
            }

            Released = true;
        }

		public void AddRef()
        {
            Interlocked.Increment(ref _refs);
		}

        [Conditional("VALIDATE")]
        public void DebugVerify(long size)
        {
            if (AllocationInfos == null)
                return;

            foreach (var allocationInfo in AllocationInfos)
            {
                for (int i = 0; i < allocationInfo.Size; i++)
                {
                    var b = *(allocationInfo.BaseAddress + i);
                    *(allocationInfo.BaseAddress + i) = b;
                }
            }

            for (int i = 0; i < size; i++)
            {
                var b = *(MapBase + i);
                *(MapBase + i) = b;
            }
        }
    }
}