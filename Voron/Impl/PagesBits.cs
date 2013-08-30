// -----------------------------------------------------------------------
//  <copyright file="PagesBits.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using Voron.Util;

namespace Voron.Impl
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
			set
			{
				if (pos < 0 || pos >= Size)
					throw new ArgumentOutOfRangeException("pos");

				unmanaged[startIndex + pos] = value;
			}
		}

		public void Set(long pos)
		{
			this[pos] = true;
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