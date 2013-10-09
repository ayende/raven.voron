// -----------------------------------------------------------------------
//  <copyright file="TransactionHeader.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Runtime.InteropServices;

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
		public int PageCount;

		[FieldOffset(20)]
		public uint Crc;

		[FieldOffset(24)] 
		public TransactionMarker Marker;
	}
}