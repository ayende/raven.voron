using System;

namespace Voron.Util
{
    public unsafe class UnmanagedBits
    {
        private readonly int* _ptr;
        private readonly long _size;
        private readonly UnmanagedBits _pages;

        public UnmanagedBits(int* ptr, long size, UnmanagedBits pages)
        {
            _ptr = ptr;
            _size = size;
            _pages = pages;
        }

	    public long Size
	    {
			get { return _size; }
	    }

        public bool this[long pos]
        {
            get
            {
                if(pos < 0 || pos >= _size)
                    throw new ArgumentOutOfRangeException("pos");

                return (_ptr[pos >> 5] & (1 << (int)(pos & 31))) != 0;
            }
            set
            {
                if (pos < 0 || pos >= _size)
                    throw new ArgumentOutOfRangeException("pos");

                if (_pages != null)
                    _pages[pos >> 12] = true;

                if (value)
                    _ptr[pos >> 5] |= (1 << (int)(pos & 31)); // '>> 5' is '/ 32', '& 31' is '% 32'
                else
                    _ptr[pos >> 5] &= ~(1 << (int)(pos & 31));
            }
        }

		public static long GetSizeOfIntArrayFor(long numberOfBits)
		{
			if (numberOfBits <= 0)
				return 0;

			return (numberOfBits - 1) / 32 + 1;
		}

		public static long GetSizeInBytesFor(long numberOfBits)
		{
			if (numberOfBits <= 0)
				return 0;

			return sizeof(int) * GetSizeOfIntArrayFor(numberOfBits);
		}
    }
}