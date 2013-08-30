// -----------------------------------------------------------------------
//  <copyright file="PagesBits.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using Voron.Util;

namespace Voron.Impl.FreeSpace
{
	public class PagesBits
	{
		private readonly UnmanagedBits unmanaged;
		private readonly long startIndex;

		public PagesBits(UnmanagedBits unmanaged, long startIndex, long size)
		{
			Size = size;
			this.unmanaged = unmanaged;
			this.startIndex = startIndex;
		}

		public long Size { get; private set; }

		public bool this[long pos]
		{
			get
			{
				if (pos < 0 || pos >= Size)
					throw new ArgumentOutOfRangeException("pos");

				return unmanaged[startIndex + pos];
			}
			internal set
			{
				if (pos < 0 || pos >= Size)
					throw new ArgumentOutOfRangeException("pos");

				unmanaged[startIndex + pos] = value;
			}
		}

		public void Clear()
		{
			for (var i = 0; i < Size; i++)
			{
				this[i] = false;
			}
		}
	}
}