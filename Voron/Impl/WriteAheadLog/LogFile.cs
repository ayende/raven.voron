// -----------------------------------------------------------------------
//  <copyright file="LogFile.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using Voron.Impl.FreeSpace;
using Voron.Trees;
using Voron.Util;

namespace Voron.Impl.WriteAheadLog
{
	public unsafe class LogFile : IDisposable
	{
		private readonly IVirtualPager _pager;
		private readonly Dictionary<long, long> _pageTranslationTable = new Dictionary<long, long>(); // TODO after restart we need to recover those values
		private long _writePage;//TODO after restart we need to recover this value

		public LogFile(IVirtualPager pager)
		{
			_pager = pager;
			_writePage = 0;
		}

		public IVirtualPager Pager
		{
			get { return _pager; }
		}

		public void Write(List<Page> pages)
		{
			long start = _writePage;
			var logEntryHeaderPage = _pager.Get(null, _writePage);
			logEntryHeaderPage.PageNumber = _writePage;

			var logEntryHeader = (LogEntryHeader*)(logEntryHeaderPage.Base + Constants.PageHeaderSize);
			logEntryHeader->PageCount = pages.Count;

			_writePage++;

			foreach (var page in pages)
			{
				var written = _pager.Write(page, _writePage);
				_pageTranslationTable[page.PageNumber] = _writePage;
				_writePage += MathUtils.DivideAndRoundUp(written, _pager.PageSize); // page could be an overflow so it can takes more than one page
			}

			_pager.Flush(start, _writePage - start);
			_pager.Sync();
		}

		public Page GetPage(Transaction tx, long pageNumber)
		{
			if (_pageTranslationTable.ContainsKey(pageNumber) == false)
				return null;

			return _pager.Get(tx, _pageTranslationTable[pageNumber]);
		}

		public void Dispose()
		{
			_pager.Dispose();
		}
	}
}