// -----------------------------------------------------------------------
//  <copyright file="TransactionMarker.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;

namespace Voron.Impl.Log
{
	[Flags]
	public enum TransactionMarker : uint
	{
		Start = 0,
		Split = 2,
		End = 4,
	}
}