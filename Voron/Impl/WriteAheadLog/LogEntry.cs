// -----------------------------------------------------------------------
//  <copyright file="LogEntry.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Collections.Generic;
using Voron.Trees;

namespace Voron.Impl.WriteAheadLog
{
	public class LogEntry
	{
		public List<Page> Pages { get; set; }
	}
}