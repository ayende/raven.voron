// -----------------------------------------------------------------------
//  <copyright file="BitBuffer.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using Voron.Util;

namespace Voron.Impl
{
	public unsafe class BitBuffer
	{
		public BitBuffer(byte* ptr, long numberOfPages)
		{
			AllBits = new UnmanagedBits(ptr, CalculateSizeForAllocation(numberOfPages), null);
			Pages = new UnmanagedBits(ptr + 1, numberOfPages, null);
			ModifiedPages = new UnmanagedBits(ptr + 1 + numberOfPages, numberOfPages, null);
		}

		public UnmanagedBits AllBits { get; set; }

		public UnmanagedBits Pages { get; set; }

		public UnmanagedBits ModifiedPages { get; set; }

		public static long CalculateSizeForAllocation(long numberOfPages)
		{
			return 1 + // dirty
			       numberOfPages + // pages
			       numberOfPages; // modified pages
		}
	}
}