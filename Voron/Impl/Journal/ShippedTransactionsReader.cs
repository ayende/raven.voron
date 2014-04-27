using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Voron.Impl.Extensions;
using Voron.Impl.Paging;
using Voron.Util;

namespace Voron.Impl.Journal
{
	public unsafe class ShippedTransactionsReader
	{
		private uint _previousTransactionCrc;
		private int currentPage;
		private readonly IVirtualPager _pager;
		private readonly Dictionary<long, JournalFile.PagePosition> _transactionPageTranslation;

		public ShippedTransactionsReader(IVirtualPager pager)
		{
			if (pager == null) throw new ArgumentNullException("pager");
			_pager = pager;
			_transactionPageTranslation = new Dictionary<long, JournalFile.PagePosition>();
		}

		public TransactionHeader? LastTransactionHeader { get; private set; }

		public Dictionary<long, JournalFile.PagePosition> TransactionPageTranslation
		{
			get { return _transactionPageTranslation; }
		}

		public void ReadTransactions(IEnumerable<TransactionToShip> shippedTransactions)
		{
			foreach (var transaction in shippedTransactions.OrderBy(x => x.Header.TransactionId))
				ReadFromShippedTransaction(transaction);
		}


		protected void ReadFromShippedTransaction(TransactionToShip transaction)
		{
			var compressedPages = (transaction.Header.CompressedSize / AbstractPager.PageSize) + (transaction.Header.CompressedSize % AbstractPager.PageSize == 0 ? 0 : 1);
			var compressedDataBuffer = new byte[compressedPages * AbstractPager.PageSize];
			transaction.CompressedData.Read(compressedDataBuffer, 0, compressedPages * AbstractPager.PageSize);

			fixed (byte* compressedDataBufferPtr = compressedDataBuffer)
			{
				var crc = Crc.Value(compressedDataBufferPtr, 0, compressedPages * AbstractPager.PageSize);
				if (transaction.Header.Crc != crc || _previousTransactionCrc != transaction.PreviousTransactionCrc)
					throw new InvalidDataException("Invalid CRC signature for transaction " + transaction.Header.TransactionId);

				_previousTransactionCrc = crc;
				var totalPages = transaction.Header.PageCount + transaction.Header.OverflowPageCount;
				_pager.EnsureContinuous(null, currentPage, totalPages + 1);
				try
				{
					LZ4.Decode64(compressedDataBufferPtr, transaction.Header.CompressedSize, _pager.AcquirePagePointer(currentPage), transaction.Header.UncompressedSize, true);
				}
				catch (Exception e)
				{
					throw new InvalidDataException("Could not de-compress, invalid data", e);
				}
			}

			var tempTransactionPageTranslaction = transaction.Header.GetTransactionToPageTranslation(_pager, ref currentPage);

			foreach (var pagePosition in tempTransactionPageTranslaction)
				TransactionPageTranslation[pagePosition.Key] = pagePosition.Value;

			if(LastTransactionHeader.HasValue && LastTransactionHeader.Value.TransactionId < transaction.Header.TransactionId)
				LastTransactionHeader = transaction.Header;
			
			currentPage += transaction.Header.PageCount;
		}

	}
}
