// -----------------------------------------------------------------------
//  <copyright file="FreeSpaceHeader.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Runtime.InteropServices;

namespace Voron.Impl.FileHeaders
{
	[StructLayout(LayoutKind.Explicit, Pack = 1)]
	public struct FreeSpaceHeader
	{
		[FieldOffset(0)]
		public long FirstBufferPageNumber;
		[FieldOffset(8)]
		public long SecondBufferPageNumber;
		[FieldOffset(16)]
		public long NumberOfTrackedPages;
		[FieldOffset(24)]
		public long NumberOfPagesTakenForTracking;
		[FieldOffset(32)]
		public int PageSize;
		[FieldOffset(36)]
		public uint Checksum;
	}
}