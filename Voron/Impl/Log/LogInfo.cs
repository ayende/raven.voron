// -----------------------------------------------------------------------
//  <copyright file="LogInfo.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Runtime.InteropServices;

namespace Voron.Impl.Log
{
	[StructLayout(LayoutKind.Explicit, Pack = 1)]
	public struct LogInfo
	{
		[FieldOffset(0)] 
		public long RecentLog;

		[FieldOffset(0)]
		public int LogFilesCount;
	}
}