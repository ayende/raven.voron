using System;
using Voron.Pagers;

namespace Voron.Journal
{
	public unsafe interface IJournalWriter : IDisposable
	{
		void WriteGather(long position, IntPtr[] pages);
		long NumberOfAllocatedPages { get;  }
		bool Disposed { get; }
	    bool DeleteOnClose { get; set; }
	    IVirtualPager CreatePager();
	    bool Read(long pageNumber, byte* buffer, int count);
	}
}
