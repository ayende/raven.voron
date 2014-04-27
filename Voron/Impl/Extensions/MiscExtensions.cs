using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Voron.Impl.Journal;

namespace Voron.Impl.Extensions
{
	public static class MiscExtensions
	{
		public static Dictionary<long, JournalFile.PagePosition> GetTransactionToPageTranslation(this TransactionHeader current,IVirtualPager pager, ref int currentPage)
		{
			var tempTransactionPageTranslaction = new Dictionary<long, JournalFile.PagePosition>();

			for (var i = 0; i < current.PageCount; i++)
			{
				Debug.Assert(pager.Disposed == false);

				var page = pager.Read(currentPage);

				tempTransactionPageTranslaction[page.PageNumber] = new JournalFile.PagePosition
				{
					JournalPos = currentPage,
					TransactionId = current.TransactionId
				};

				if (page.IsOverflow)
				{
					var numOfPages = pager.GetNumberOfOverflowPages(page.OverflowSize);
					currentPage += numOfPages;
				}
				else
				{
					currentPage++;
				}
			}
			return tempTransactionPageTranslaction;
		}
	}
}
