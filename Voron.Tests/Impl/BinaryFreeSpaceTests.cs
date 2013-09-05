//TODO arek
//// -----------------------------------------------------------------------
////  <copyright file="FreePagesRepositoryTests.cs" company="Hibernating Rhinos LTD">
////      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
////  </copyright>
//// -----------------------------------------------------------------------

//using System;
//using System.IO;
//using Voron.Impl;
//using Voron.Impl.FreeSpace;
//using Xunit;

//namespace Voron.Tests.Impl
//{
//	public class BinaryFreeSpaceTests : IDisposable
//	{


























//		[Fact]
//		public void ReturnedBufferShouldAlwaysHaveCleanModifiedPages()
//		{
//			using (var freePages = new BinaryFreeSpace("free-space", 10))
//			{
//				var tx1 = 1;

//				var buffer = freePages.GetBufferForTransaction(tx1);

//				buffer.SetPage(0, true); // will mark modified bits
//				buffer.SetPage(3, true);
//				buffer.SetPage(9, true);

//				buffer = freePages.GetBufferForTransaction(tx1); // should clean modified pages

//				for (int i = 0; i < buffer.ModifiedPages.Size; i++)
//				{
//					Assert.False(buffer.ModifiedPages[i]);
//				}
//			}
//		}

//		[Fact]
//		public void SettingNewPageAsFreeShouldAlsoMarkItInModifiedPages()
//		{
//			using (var freePages = new BinaryFreeSpace("free-space", 10))
//			{
//				var tx = 1;

//				var buffer = freePages.GetBufferForTransaction(tx);

//				buffer.SetPage(1, true);
//				buffer.SetPage(5, true);
//				buffer.SetPage(6, true);

//				Assert.True(buffer.ModifiedPages[1]);
//				Assert.True(buffer.ModifiedPages[5]);
//				Assert.True(buffer.ModifiedPages[6]);
//			}
//		}

//		[Fact]
//		public void WhenYouGetFreePagesItShouldMarkItInModifiedPages()
//		{
//			using (var freePages = new BinaryFreeSpace("free-space", 10))
//			{
//				var tx = 1;

//				var buffer = freePages.GetBufferForTransaction(tx);

//				buffer.SetPage(1, true);
//				buffer.SetPage(2, true);

//				buffer.Find(2);

//				Assert.True(buffer.ModifiedPages[1]);
//				Assert.True(buffer.ModifiedPages[2]);
//			}
//		}

//		private void DeleteFiles()
//		{
//			if (File.Exists("free-space-0"))
//				File.Delete("free-space-0");

//			if (File.Exists("free-space-1"))
//				File.Delete("free-space-1");
//		}

//		public void Dispose()
//		{
//			DeleteFiles();
//		}
//	}
//}