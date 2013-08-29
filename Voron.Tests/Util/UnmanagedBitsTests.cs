// -----------------------------------------------------------------------
//  <copyright file="UnmanagedBitsTests.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using Voron.Util;
using Xunit;

namespace Voron.Tests.Util
{
	public class UnmanagedBitsTests
	{
		[Fact]
		public void CanSetAndGetTheSameValues()
		{
			var bytes = new int[UnmanagedBits.GetSizeInBytesToAllocate(128)];

			unsafe
			{
				fixed (int* ptr = bytes)
				{
					var bits = new UnmanagedBits(ptr, 128, null);

					var modeFactor = new Random().Next(1, 7);

					for (int i = 0; i < 128; i++)
					{
						bits[i] = (i % modeFactor) == 0;
					}

					for (int i = 0; i < 128; i++)
					{
						Assert.Equal(bits[i], (i % modeFactor) == 0);
					}
				}
			}
		}
	}
}