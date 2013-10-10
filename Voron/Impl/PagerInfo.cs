// -----------------------------------------------------------------------
//  <copyright file="PagerInfo.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
namespace Voron.Impl
{
	public class PagerInfo
	{
		public int PageSize { get; set; }
		public int MaxNodeSize { get; set; }
		public int PageMaxSpace { get; set; }
		public int PageMinSpace { get; set; } 
	}
}