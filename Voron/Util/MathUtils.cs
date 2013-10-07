namespace Voron.Util
{
	public static class MathUtils
	{
		public static long DivideAndRoundUp(long numerator, long denominator)
		{
			return (numerator + denominator - 1) / denominator;
		}
	}
}