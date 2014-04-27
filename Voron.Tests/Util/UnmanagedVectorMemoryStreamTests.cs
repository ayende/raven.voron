using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;
using Voron.Impl;
using Voron.Impl.Journal;
using Xunit;
using Xunit.Extensions;

namespace Voron.Tests.Util
{
	public unsafe class UnmanagedVectorMemoryStreamTests
	{
		[StructLayout(LayoutKind.Sequential)]
		private struct Dummy
		{
			public long Number1 { get; set; }
			public long Number2 { get; set; }
		}

#if DEBUG
		[Fact]
		public void Read_should_work_correctly()
		{
			var random = new Random();
			var arr1 = new byte[25];
			var arr2 = new byte[25];
			var arr3 = new byte[25];

			random.NextBytes(arr1);
			random.NextBytes(arr2);
			random.NextBytes(arr3);

			fixed (byte* parr1 = arr1)
			fixed (byte* parr2 = arr2)
			fixed (byte* parr3 = arr3)
			{
				var array = new[] {parr1, parr2, parr3};
				
				var stream = new UnmanagedVectorMemoryStream(array, 1, 25);
				var expectedReadOutut = new byte[50];				
				fixed (byte* pExpectedReadOutut = expectedReadOutut)
				{
					NativeMethods.memcpy(pExpectedReadOutut, parr2, 25);
					NativeMethods.memcpy(pExpectedReadOutut + 25, parr3, 25);
				}

				var readOutput = new byte[50];
				var debugReadOutput = stream.DebugReadAllData(25);
				stream.Read(readOutput, 0, 50);

				Assert.Equal(debugReadOutput, readOutput);
				Assert.Equal(expectedReadOutut, debugReadOutput);
				Assert.Equal(expectedReadOutut, readOutput);
			}
			
		}
#endif

		[Theory]
		[InlineData(0)]
		[InlineData(3)]
		[InlineData(12)]
		public void TestReadFromMultiplePages_OffsetFromBegin(int offset)
		{
			var obj1 = new Dummy { Number1 = 123, Number2 = 456 };
			var obj2 = new Dummy { Number1 = 789, Number2 = 123 };
			var obj3 = new Dummy { Number1 = 901, Number2 = 234 };
			var dummySize = Marshal.SizeOf(typeof(Dummy));

			var data = new byte*[3];
			data[0] = (byte*)&obj1;
			data[1] = (byte*)&obj2;
			data[2] = (byte*)&obj3;
			
			var dataBufferPtr = IntPtr.Zero;
			try
			{
				dataBufferPtr = Marshal.AllocHGlobal((dummySize * 3) - offset);
				var dataBuffer = (byte*)dataBufferPtr.ToPointer();

				NativeMethods.memcpy(dataBuffer, data[0] + offset, dummySize - offset);
				NativeMethods.memcpy(dataBuffer + dummySize - offset, data[1], dummySize);
				NativeMethods.memcpy(dataBuffer + (dummySize * 2) - offset, data[2], dummySize);
				
				using (var stream = new UnmanagedVectorMemoryStream(data, 0,dummySize))
				{
					var readData = new byte[(dummySize * 3) - offset];
					stream.Seek(offset, SeekOrigin.Begin);

					stream.Read(readData, 0, (dummySize * 3) - offset);
					fixed (byte* readDataPtr = readData)
					{
						var fetchedObj2Ptr = (Dummy*)(readDataPtr + dummySize - offset);
						var fetchedObj3Ptr = (Dummy*)(readDataPtr + ((dummySize * 2) - offset));

						Assert.Equal(obj2.Number1, fetchedObj2Ptr->Number1);
						Assert.Equal(obj2.Number2, fetchedObj2Ptr->Number2);

						Assert.Equal(obj3.Number1, fetchedObj3Ptr->Number1);
						Assert.Equal(obj3.Number2, fetchedObj3Ptr->Number2);
					}
				}
			}
			finally
			{
				Marshal.FreeHGlobal(dataBufferPtr);
			}
		}


		[Fact]
		public void TestReadFromMultiplePages()
		{
			var obj1 = new Dummy { Number1 = 123, Number2 = 456 };
			var obj2 = new Dummy { Number1 = 789, Number2 = 123 };
			var obj3 = new Dummy { Number1 = 901, Number2 = 234 };
			var dummySize = Marshal.SizeOf(typeof(Dummy));
			Dummy* ptrObj1 = &obj1;
			Dummy* ptrObj2 = &obj2;
			Dummy* ptrObj3 = &obj3;
			{
				var data = new byte*[3];
				data[0] = (byte*)ptrObj1;
				data[1] = (byte*)ptrObj2;
				data[2] = (byte*)ptrObj3;


				var dataBufferPtr = IntPtr.Zero;
				try
				{
					dataBufferPtr = Marshal.AllocHGlobal(dummySize*3 + 1);
					var dataBuffer = (byte*) dataBufferPtr.ToPointer();
					NativeMethods.memcpy(dataBuffer, data[0], dummySize);
					NativeMethods.memcpy(dataBuffer + dummySize, data[1], dummySize);
					NativeMethods.memcpy(dataBuffer + (dummySize*2), data[2], dummySize);

					using (var stream = new UnmanagedVectorMemoryStream(data,0, dummySize))
					{
						var readData = new byte[dummySize*3];
						stream.Read(readData, 0, dummySize*3);
						fixed (byte* readDataPtr = readData)
						{
//						Assert.Equal(0, NativeMethods.memcmp(dataBuffer, readDataPtr, dummySize * 3));
							var fetchedObj1Ptr = (Dummy*) readDataPtr;
							Assert.Equal(obj1.Number1, fetchedObj1Ptr->Number1);
							Assert.Equal(obj1.Number2, fetchedObj1Ptr->Number2);

							var fetchedObj2Ptr = (Dummy*) (readDataPtr + dummySize);
							Assert.Equal(obj2.Number1, fetchedObj2Ptr->Number1);
							Assert.Equal(obj2.Number2, fetchedObj2Ptr->Number2);

							var fetchedObj3Ptr = (Dummy*) (readDataPtr + (dummySize*2));
							Assert.Equal(obj3.Number1, fetchedObj3Ptr->Number1);
							Assert.Equal(obj3.Number2, fetchedObj3Ptr->Number2);
						}
					}
				}
				finally
				{
					Marshal.FreeHGlobal(dataBufferPtr);
				}
			}
		}


		[Fact]
		public void TestReadFromSinglePage_FullCopy()
		{
			var obj = new Dummy {Number1 = 123, Number2 = 456};
			var dummySize = Marshal.SizeOf(typeof (Dummy));
		
			Dummy* objPtr = &obj;
			var data = new byte*[1];
			data[0] = (byte*) objPtr;

			using (var stream = new UnmanagedVectorMemoryStream(data,0, dummySize))
			{
				var readData = new byte[dummySize];
				stream.Read(readData, 0, dummySize);
				fixed (byte* readDataPtr = readData)
					Assert.Equal(0, NativeMethods.memcmp(data[0], readDataPtr, dummySize));
			}
		}

		[Fact]
		public void TestReadFromSinglePage_PartialCopy()
		{
			var obj = new Dummy { Number1 = 123, Number2 = 456 };
			var dummySize = Marshal.SizeOf(typeof(Dummy));

			Dummy* objPtr = &obj;
			var data = new byte*[1];
			data[0] = (byte*)objPtr;
			var offset = dummySize / 3;
			using (var stream = new UnmanagedVectorMemoryStream(data,0, dummySize))
			{
				stream.Seek(offset, SeekOrigin.Begin);
				var readData = new byte[dummySize];
				stream.Read(readData, 0, dummySize);
				fixed (byte* readDataPtr = readData)
					Assert.Equal(0, NativeMethods.memcmp(data[0] + offset, readDataPtr, dummySize - offset));
			}
		}
	
	}
}
