using System;

namespace Voron.Impl
{
	public interface IBufferProvider : IDisposable
	{
		void IncreaseSize(long size);
		unsafe byte* GetBufferPointer();
		void Release();
	}
}