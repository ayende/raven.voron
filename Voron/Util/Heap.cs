using System;
using System.ComponentModel;
using System.Runtime.InteropServices;

namespace Voron.Util
{
    public unsafe class Heap : IDisposable
    {
        [DllImport("kernel32.dll", SetLastError = true)]
        private static extern IntPtr HeapCreate(uint flOptions, UIntPtr dwInitialsize, UIntPtr dwMaximumSize);

        [DllImport("kernel32.dll", SetLastError = true)]
        private static extern IntPtr HeapAlloc(IntPtr hHeap, uint dwFlags, UIntPtr dwSize);

        [DllImport("kernel32.dll", SetLastError = true)]
        private static extern bool HeapFree(IntPtr hHeap, uint dwFlags, IntPtr lpMem);

        [DllImport("kernel32.dll", SetLastError = true)]
        private static extern bool HeapDestroy(IntPtr hHeap);

        private IntPtr _heap;

        public Heap()
        {
            _heap = HeapCreate(0, UIntPtr.Zero, UIntPtr.Zero);
            if (_heap == IntPtr.Zero)
                throw new Win32Exception();
        }

        public byte* Allocate(int size)
        {
            var ptr = (byte*) HeapAlloc(_heap, 0, (UIntPtr) size);
            if (ptr == null)
                throw new OutOfMemoryException("Cannot allocate memory of size " + size + " from this heap");
            return ptr;
        }

        public void Free(byte* ptr)
        {
            if(HeapFree(_heap, 0, (IntPtr) ptr) == false)
                throw new Win32Exception();
        }

        ~Heap()
        {
            if (_heap != IntPtr.Zero)
                HeapDestroy(_heap);
        }

        public void Dispose()
        {
            if(HeapDestroy(_heap) == false)
                throw new Win32Exception();
            _heap = IntPtr.Zero;
            GC.SuppressFinalize(this);
        }
    }
}