// -----------------------------------------------------------------------
//  <copyright file="TransactionStateMarker.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;

namespace Voron.Impl.Log
{
	[Flags]
	public enum TransactionStateMarker : uint
	{
		Start = 0,
		Split = 2,
		Commit = 4,
	}
}