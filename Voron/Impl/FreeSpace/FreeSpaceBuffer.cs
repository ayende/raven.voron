// -----------------------------------------------------------------------
//  <copyright file="BitBuffer.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Diagnostics;
using Voron.Util;

namespace Voron.Impl.FreeSpace
{
	public unsafe class FreeSpaceBuffer
	{
		internal long lastSearchPosition = 0;

		public FreeSpaceBuffer(int* ptr, long numberOfPages)
		{
			AllBits = new UnmanagedBits(ptr, 1 + numberOfPages + Math.Min(1, numberOfPages / 4096));
			FreePages = new PagesBits(AllBits, 1, numberOfPages);
			ModifiedPages = new PagesBits(AllBits, numberOfPages, Math.Min(1, numberOfPages / 4096));
		}

		public UnmanagedBits AllBits { get; private set; }

		public PagesBits FreePages { get; private set; }

		public PagesBits ModifiedPages { get; set; }

		public void SetPage(long pageNumber, bool free)
		{
			FreePages[pageNumber] = free;
			ModifiedPages[pageNumber / 4096] = true;
		}

		public bool IsDirty
		{
			get { return AllBits[0]; }
			set { AllBits[0] = value; }
		}

		public IList<long> Find(int numberOfFreePages)
		{
			var result = GetContinuousRangeOfFreePages(numberOfFreePages);

			if (result != null)
			{
				foreach (var freePageNumber in result)
				{
					SetPage(freePageNumber, false); // mark returned pages as busy
				}
			}

			return result;
		}

		internal IList<long> GetContinuousRangeOfFreePages(int numberOfPagesToGet)
		{
			Debug.Assert(numberOfPagesToGet > 0);

			var range = new List<long>();

			if (lastSearchPosition >= FreePages.Size)
				lastSearchPosition = 0;

			var page = lastSearchPosition;

			for (; page < FreePages.Size; page++)
			{
				if (FreePages[page]) // free page
				{
					if (range.Count == 0 || range[range.Count - 1] == page - 1) // when empty or continuous
					{
						range.Add(page);

						if (range.Count == numberOfPagesToGet)
						{
							page++; // next time start searching from a next page
							break;
						}

						continue; // continue looking for next free page in continuous range
					}
				}

				range.Clear();
			}

			lastSearchPosition = page;

			Debug.Assert(range.Count <= numberOfPagesToGet);

			return range.Count == numberOfPagesToGet ? range : null;
		}

		public static long CalculateSizeInBytesForAllocation(long numberOfPages)
		{
			return UnmanagedBits.GetSizeInBytesFor(
				1 + // dirty bit
				numberOfPages + // pages
				Math.Min(1, numberOfPages / 4096)); // modified pages
		}
	}
}