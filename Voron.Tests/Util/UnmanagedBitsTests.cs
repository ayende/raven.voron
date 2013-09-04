// -----------------------------------------------------------------------
//  <copyright file="UnmanagedBitsTests.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using Voron.Impl.FreeSpace;
using Xunit;

namespace Voron.Tests.Util
{
	public class UnmanagedBitsTests
	{
		[Fact]
		public void CanSetAndGetTheSameValues()
		{
			var bytes = new byte[UnmanagedBits.CalculateSizeInBytesForAllocation(128, 4096)];

			unsafe
			{
				fixed (byte* ptr = bytes)
				{
					var bits = new UnmanagedBits(ptr, 0, bytes.Length * sizeof(byte), 128, 4096);

					var modeFactor = new Random().Next(1, 7);

					for (int i = 0; i < 128; i++)
					{
						bits.MarkPage(i, (i % modeFactor) == 0);
					}

					for (int i = 0; i < 128; i++)
					{
						Assert.Equal(bits.IsFree(i), (i % modeFactor) == 0);
					}
				}
			}
		}
	}
}