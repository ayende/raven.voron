using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using Voron.Trees;
using Voron.Util;

namespace Voron.Impl
{
	/// <summary>
	/// This class implements the page pool for in flight transaction information
	/// Pages allocated from here are expected to live after the write transaction that 
	/// created them. The pages will be kept around until the flush for the journals
	/// send them to the data file.
	/// </summary>
	public unsafe class ScratchBufferPool : IDisposable
	{
		private readonly IVirtualPager _scratchPager;
		private readonly ConcurrentDictionary<long, LinkedList<long>> _freePagesBySize = new ConcurrentDictionary<long, LinkedList<long>>();
		private readonly ConcurrentDictionary<long, PageFromScratchBuffer> _allocatedPages = new ConcurrentDictionary<long, PageFromScratchBuffer>();
		private readonly ConcurrentDictionary<long, PageFromScratchBuffer> _pagesToFreeAfterReading = new ConcurrentDictionary<long, PageFromScratchBuffer>();
		private long _lastUsedPage;

		public ScratchBufferPool(StorageEnvironment env)
		{
			_scratchPager = env.Options.CreateScratchPager("scratch.buffers");
			_scratchPager.AllocateMorePages(null, env.Options.InitialLogFileSize);
		}

		public PagerState PagerState { get { return _scratchPager.PagerState; }}

		public PageFromScratchBuffer Allocate(Transaction tx, int numberOfPages)
		{
			var size = Utils.NearestPowerOfTwo(numberOfPages);
			LinkedList<long> list;
			if (_freePagesBySize.TryGetValue(size, out list) && list.Count > 0)
			{
				var position = list.First.Value;
				list.RemoveFirst();
			    var pageFromScratchBuffer = new PageFromScratchBuffer
			    {
			        PositionInScratchBuffer = position,
			        Size = size,
			        NumberOfPages = numberOfPages
			    };
                
				if (_allocatedPages.TryAdd(position, pageFromScratchBuffer) == false)
					throw new InvalidOperationException(
						string.Format("Could add item to allocated pages collection. Page position: {0}, taken from from freed pages",
						              position));
			    
				return pageFromScratchBuffer;
			}
			// we don't have free pages to give out, need to allocate some
			_scratchPager.EnsureContinuous(tx, _lastUsedPage, (int) size);

			var result = new PageFromScratchBuffer
			{
				PositionInScratchBuffer = _lastUsedPage,
				Size = size,
				NumberOfPages = numberOfPages
			};

            if (_allocatedPages.TryAdd(_lastUsedPage, result) == false)
            {
				throw new InvalidOperationException(
						string.Format("Could add item to allocated pages collection. Page position: {0}, new allocation.",
									  _lastUsedPage));
            }

			_lastUsedPage += size;

			return result;
		}

		public void Free(PageFromScratchBuffer page)
		{
			PageFromScratchBuffer _;
			if (_allocatedPages.TryGetValue(page.PositionInScratchBuffer, out _) == false)
				throw new InvalidOperationException("Attempt to free page that wasn't currently allocated: " + page);
			
			if (_allocatedPages.TryRemove(page.PositionInScratchBuffer, out _) == false)
				throw new InvalidOperationException("Could not remove page from allocated pages collection: " + page);

			var pagesToFree = _pagesToFreeAfterReading.Values.Where(x => x.IsBeingRead == false).ToList();

			if (page.IsBeingRead)
			{
				_pagesToFreeAfterReading[page.PositionInScratchBuffer] = page;
			}
			else
				pagesToFree.Add(page);

			foreach (var toFree in pagesToFree)
			{
				Debug.Assert(toFree.IsBeingRead == false);

				LinkedList<long> list;
				if (_freePagesBySize.TryGetValue(toFree.Size, out list) == false)
				{
					list = new LinkedList<long>();
					_freePagesBySize[toFree.Size] = list;
				}
				list.AddFirst(toFree.PositionInScratchBuffer);

				_pagesToFreeAfterReading.TryRemove(toFree.PositionInScratchBuffer, out _);
			}
		}

		public void Dispose()
		{
			_scratchPager.Dispose();
		}

		public Page ReadPage(PageFromScratchBuffer p, PagerState pagerState = null)
		{
			return _scratchPager.Read(p.PositionInScratchBuffer, pagerState);
		}

		public byte* AcquirePagePointer(long p)
		{
			return _scratchPager.AcquirePagePointer(p);
		}
	}

	public class PageFromScratchBuffer
	{
		public long PositionInScratchBuffer;
		public long Size;
		public int NumberOfPages;
		private int _readReferences;

		public override bool Equals(object obj)
		{
			if (ReferenceEquals(null, obj)) return false;
			if (ReferenceEquals(this, obj)) return true;
			if (obj.GetType() != this.GetType()) return false;

			var other = (PageFromScratchBuffer) obj;

			return PositionInScratchBuffer == other.PositionInScratchBuffer && Size == other.Size && NumberOfPages == other.NumberOfPages;
		}

		public override int GetHashCode()
		{
			unchecked
			{
				var hashCode = PositionInScratchBuffer.GetHashCode();
				hashCode = (hashCode * 397) ^ Size.GetHashCode();
				hashCode = (hashCode * 397) ^ NumberOfPages;
				return hashCode;
			}
		}

		public bool IsBeingRead
		{
			get
			{
				return Thread.VolatileRead(ref _readReferences) > 0;
			}
		}

		public void AddReadRef()
		{
			Interlocked.Increment(ref _readReferences);
		}

		public void ReleaseReadRef()
		{
			Interlocked.Decrement(ref _readReferences);
		}
	}
}