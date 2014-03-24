using System;
using System.Diagnostics;
using System.IO;
using Voron;
using Voron.Util.Conversion;

namespace Hibernating.Consensus.Messages
{
	public struct EntryId : IComparable<EntryId>
	{
		public EntryId(long term, long index) : this()
		{
			Term = term;
			Index = index;
		}

		public EntryId(Slice slice) : this()
		{
			Debug.Assert(slice.Size == 16);
			var bytes = new byte[16];
			slice.CopyTo(bytes);

			Term = EndianBitConverter.Big.ToInt64(bytes, 0);
			Index = EndianBitConverter.Big.ToInt64(bytes, 8);
		}

		public long Term { get; private set; }

		public long Index { get; private set; }

		public bool Equals(EntryId other)
		{
			return Term == other.Term && Index == other.Index;
		}

		public override bool Equals(object obj)
		{
			if (ReferenceEquals(null, obj)) return false;
			return obj is EntryId && Equals((EntryId) obj);
		}

		public override int GetHashCode()
		{
			unchecked
			{
				return (Term.GetHashCode()*397) ^ Index.GetHashCode();
			}
		}

		public int CompareTo(EntryId other)
		{
			var r = Term - other.Term;
			if (r != 0)
				return (int)r;
			return (int) (Index - other.Index);
		}

		public override string ToString()
		{
			return string.Format("Term: {0}, Index: {1}", Term, Index);
		}

		public byte[] ToBytes()
		{
			var buffer = new byte[16];

			EndianBitConverter.Big.CopyBytes(Term, buffer, 0);
			EndianBitConverter.Big.CopyBytes(Index, buffer, 8);

			return buffer;
		}

		public Stream ToStream()
		{
			return new MemoryStream(ToBytes());
		}

		public Slice ToSlice()
		{
			return new Slice(ToBytes());
		}

		public static bool operator ==(EntryId x, EntryId y)
		{
			return x.Index == y.Index && x.Term == y.Term;
		}

		public static bool operator !=(EntryId x, EntryId y)
		{
			return !(x == y);
		}


		public static bool operator >(EntryId x, EntryId y)
		{
			if (x.Term > y.Term)
				return true;
			if (x.Term < y.Term)
				return false;
			return x.Index > y.Index;
		}

		public static bool operator >=(EntryId x, EntryId y)
		{
			if (x.Term > y.Term)
				return true;
			if (x.Term < y.Term)
				return false;
			return x.Index >= y.Index;
		}

		public static bool operator <=(EntryId x, EntryId y)
		{
			if (x.Term < y.Term)
				return true;
			if (x.Term > y.Term)
				return false;
			return x.Index <= y.Index;
		}

		public static bool operator <(EntryId x, EntryId y)
		{
			if (x.Term < y.Term)
				return true;
			if (x.Term > y.Term)
				return false;
			return x.Index < y.Index;
		}
	}
}