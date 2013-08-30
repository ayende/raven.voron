// -----------------------------------------------------------------------
//  <copyright file="BitBuffer.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Diagnostics;

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

	}
}