// -----------------------------------------------------------------------
//  <copyright file="BitBuffer.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Collections.Generic;
using System.Diagnostics;
using Voron.Util;

namespace Voron.Impl
{
	public unsafe class BitBuffer
	{
		internal long lastSearchPosition = 0;

		public BitBuffer(int* ptr, long numberOfPages)
		{
			AllBits = new UnmanagedBits(ptr, 1 + numberOfPages + numberOfPages, null);
			Pages = new PagesBits(AllBits, 1, numberOfPages);
			ModifiedPages = new PagesBits(AllBits, 1 + numberOfPages, numberOfPages);

			Ptr = ptr;
		}

		public UnmanagedBits AllBits { get; private set; }

		public PagesBits Pages { get; private set; }

		public PagesBits ModifiedPages { get; set; }

		public int* Ptr { get; private set; }

		public bool IsDirty
		{
			get { return AllBits[0]; }
			set { AllBits[0] = value; }
		}

		public IList<long> GetContinuousRangeOfFreePages(int numberOfPagesToGet)
		{
			Debug.Assert(numberOfPagesToGet > 0);

			var range = new List<long>();

			if (lastSearchPosition >= Pages.Size)
				lastSearchPosition = 0;

			var page = lastSearchPosition;

			for (; page < Pages.Size; page++)
			{
				if (Pages[page]) // free page
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
				numberOfPages); // modified pages
		}
	}
}