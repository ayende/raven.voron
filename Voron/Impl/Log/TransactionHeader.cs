// -----------------------------------------------------------------------
//  <copyright file="TransactionHeader.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Runtime.InteropServices;
using Voron.Impl.FileHeaders;

namespace Voron.Impl.Log
{
	[StructLayout(LayoutKind.Explicit, Pack = 1)]
	public struct TransactionHeader
	{
		[FieldOffset(0)]
		public long TxId;

		[FieldOffset(8)]
		public long NextPageNumber;

		[FieldOffset(16)]
		public long LastPageNumber;

		[FieldOffset(24)]
		public int PageCount;

		[FieldOffset(28)]
		public uint Crc;

		[FieldOffset(32)] 
		public TransactionMarker Marker;

		[FieldOffset(36)]
		public TreeRootHeader Root;
	}
}