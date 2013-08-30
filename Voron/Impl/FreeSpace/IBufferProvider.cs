using System;

namespace Voron.Impl.FreeSpace
{
	public interface IBufferProvider : IDisposable
	{
		void IncreaseSize(long size);
		unsafe int* GetBufferPointer();
		void Release();
	}
}