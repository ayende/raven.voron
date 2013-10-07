// -----------------------------------------------------------------------
//  <copyright file="LogEntryHeader.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Runtime.InteropServices;

namespace Voron.Impl.Log
{
	[StructLayout(LayoutKind.Explicit, Pack = 1)]
	public struct LogEntryHeader
	{
		[FieldOffset(0)]
		public int PageCount;
	}
}