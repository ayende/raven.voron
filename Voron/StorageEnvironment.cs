using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using Voron.Debugging;
using Voron.Impl;
using Voron.Impl.FileHeaders;
using Voron.Impl.FreeSpace;
using Voron.Impl.Log;
using Voron.Trees;

namespace Voron
{
    public unsafe class StorageEnvironment : IDisposable
    {
        private readonly ConcurrentDictionary<long, Transaction> _activeTransactions =
            new ConcurrentDictionary<long, Transaction>();

        private readonly ConcurrentDictionary<string, Tree> _trees
            = new ConcurrentDictionary<string, Tree>(StringComparer.OrdinalIgnoreCase);

        private readonly bool _ownsPagers;
        private readonly IVirtualPager _dataPager;
        private readonly SliceComparer _sliceComparer;

        private readonly SemaphoreSlim _txWriter = new SemaphoreSlim(1);

        private long _transactionsCounter;

        public TransactionMergingWriter Writer { get; private set; }

	    private readonly WriteAheadLog _log;
	    private Timer _logApplyingTimer;

	    public SnapshotReader CreateSnapshot()
        {
            return new SnapshotReader(NewTransaction(TransactionFlags.Read));
        }

		public StorageEnvironment(IVirtualPager dataPager, Func<string, IVirtualPager> createLogFilePager, bool ownsPagers = true)
		{
			try
			{
				_dataPager = dataPager;
				_ownsPagers = ownsPagers;
				_sliceComparer = NativeMethods.memcmp;
				FreeSpaceHandling = new BinaryFreeSpaceStrategy(n => new IntPtr(_dataPager.AcquirePagePointer(n)));

				_log = new WriteAheadLog(this, createLogFilePager, _dataPager, disposeLogFiles: _ownsPagers);
				
				Setup();

				Root.Name = "Root";
				
                Writer = new TransactionMergingWriter(this);

				_logApplyingTimer = new Timer(FlushLogToDataFile);
				_logApplyingTimer.Change(TimeSpan.FromMinutes(5), TimeSpan.FromMinutes(5));
			}
            catch (Exception)
            {
                Dispose();
	            throw;
            }
        }

        private void Setup()
        {
			
	        if (_dataPager.NumberOfAllocatedPages == 0)
            {
				//var freeSpaceHeader = new FreeSpaceHeader
				//	{
				//		FirstBufferPageNumber = 2,
				//		SecondBufferPageNumber = 3,
				//		NumberOfPagesTakenForTracking = 1,
				//		NumberOfTrackedPages = _dataPager.NumberOfAllocatedPages,
				//		PageSize = _dataPager.PageSize,
				//		Checksum = Crc.ForEmptyInitialBuffer(_dataPager.PageSize)
				//	};

				//FreeSpaceHandling.Initialize(&freeSpaceHeader, true);

				NextPageNumber = 2;

				using (var tx = new Transaction(_log, _dataPager, this, _transactionsCounter + 1, TransactionFlags.ReadWrite, null))
				{
					var root = Tree.Create(tx, _sliceComparer);

					// important to first create the tree, then set it on the env
					Root = root;

					tx.UpdateRoot(root);

                    tx.Commit();
                }

                return;
            }
            // existing db, let us load it

			// the first two pages are allocated for double buffering tx commits
			FileHeader* entry = FindLatestFileHeadeEntry();
			TreeRootHeader* treeRootHeader = &entry->Root;

			NextPageNumber = entry->LastPageNumber + 1;
			_transactionsCounter = entry->TransactionId + 1;
	        
	        TransactionHeader* lastTxHeader;
	        if (_log.TryRecover(entry, out lastTxHeader))
	        {
		        NextPageNumber = lastTxHeader->LastPageNumber + 1;
		        _transactionsCounter = lastTxHeader->TxId + 1;
		        treeRootHeader = &lastTxHeader->Root;
	        }

			using (var tx = new Transaction(_log, _dataPager, this, _transactionsCounter + 1, TransactionFlags.ReadWrite, null))
			{
				var root = Tree.Open(tx, _sliceComparer, treeRootHeader);

				// important to first create the  tree, then set it on the env
				Root = root;

				var freeSpaceHeader = &entry->FreeSpace;// TODO
				FreeSpaceHandling.Initialize(freeSpaceHeader);
				FreeSpaceHandling.RecoverBuffers();

				tx.Commit();
			}
		}

        public long NextPageNumber { get; set; }

        public SliceComparer SliceComparer
        {
            get { return _sliceComparer; }
        }

        public Tree Root { get; private set; }

        public long OldestTransaction
        {
            get { return _activeTransactions.Keys.OrderBy(x => x).FirstOrDefault(); }
        }

        public int PageSize
        {
            get { return _dataPager.PageSize; }
        }

		public BinaryFreeSpaceStrategy FreeSpaceHandling { get; set; }

        public Tree GetTree(Transaction tx, string name)
        {
            Tree tree;
            if (_trees.TryGetValue(name, out tree))
                return tree;

            if (tx != null && tx.ModifiedTrees.TryGetValue(name, out tree))
                return tree;

            throw new InvalidOperationException("No such tree: " + name);
        }


        public void DeleteTree(Transaction tx, string name)
        {
            if (tx.Flags == (TransactionFlags.ReadWrite) == false)
                throw new ArgumentException("Cannot create a new tree with a read only transaction");

            Tree tree;
            if (_trees.TryGetValue(name, out tree) == false)
                return;

            foreach (var page in tree.AllPages(tx))
            {
                tx.FreePage(page);
            }

            Root.Delete(tx, name);

            tx.ModifiedTrees.Add(name, null);
        }

        public Tree CreateTree(Transaction tx, string name)
        {
            if (tx.Flags == (TransactionFlags.ReadWrite) == false)
                throw new ArgumentException("Cannot create a new tree with a read only transaction");

            Tree tree;
            if (_trees.TryGetValue(name, out tree) ||
                tx.ModifiedTrees.TryGetValue(name, out tree))
                return tree;

            Slice key = name;

            // we are in a write transaction, no need to handle locks
            var header = (TreeRootHeader*)Root.DirectRead(tx, key);
            if (header != null)
            {
                tree = Tree.Open(tx, _sliceComparer, header);
                tree.Name = name;
                tx.ModifiedTrees.Add(name, tree);
                return tree;
            }

            tree = Tree.Create(tx, _sliceComparer);
            tree.Name = name;
            var space = Root.DirectAdd(tx, key, sizeof(TreeRootHeader));
            tree.State.CopyTo((TreeRootHeader*)space);

            tx.ModifiedTrees.Add(name, tree);

            return tree;
        }

		public void Dispose()
		{
			foreach (var activeTransaction in _activeTransactions)
			{
				activeTransaction.Value.Dispose();
			}

			if (_ownsPagers)
				_dataPager.Dispose();

			_log.Dispose();
		}

        private FileHeader* FindLatestFileHeadeEntry()
        {
            Page fst = _dataPager.Read(null, 0);
            Page snd = _dataPager.Read(null, 1);

            FileHeader* e1 = GetFileHeaderFrom(fst);

	        return e1;
			//TODO
            FileHeader* e2 = GetFileHeaderFrom(snd);

            FileHeader* entry = e1;
            if (e2->LogInfo.DataFlushCounter > e1->LogInfo.DataFlushCounter)
            {
                entry = e2;
            }
            return entry;
        }

        private FileHeader* GetFileHeaderFrom(Page p)
        {
			var fileHeader = ((FileHeader*)p.Base + Constants.PageHeaderSize);
            if (fileHeader->MagicMarker != Constants.MagicMarker)
                throw new InvalidDataException(
                    "The header page did not start with the magic marker, probably not a db file");
            if (fileHeader->Version != Constants.CurrentVersion)
                throw new InvalidDataException("This is a db file for version " + fileHeader->Version +
                                               ", which is not compatible with the current version " +
                                               Constants.CurrentVersion);
            if (fileHeader->LastPageNumber >= _dataPager.NumberOfAllocatedPages)
                throw new InvalidDataException("The last page number is beyond the number of allocated pages");
            if (fileHeader->TransactionId < 0)
                throw new InvalidDataException("The transaction number cannot be negative");
            return fileHeader;
        }

		public Transaction NewTransaction(TransactionFlags flags)
		{
			bool txLockTaken = false;

			try
			{
				UnmanagedBits freeSpaceBuffer = null;
				long txId = _transactionsCounter;
				if (flags == (TransactionFlags.ReadWrite))
				{
					txId = _transactionsCounter + 1;
					_txWriter.Wait();
					txLockTaken = true;

					freeSpaceBuffer = FreeSpaceHandling.GetBufferForNewTransaction(txId);
				}
				var tx = new Transaction(_log, _dataPager, this, txId, flags, freeSpaceBuffer);
				_activeTransactions.TryAdd(txId, tx);
				var state = _dataPager.TransactionBegan();
				tx.AddPagerState(state);

				if (flags == TransactionFlags.ReadWrite)
				{
					tx.AfterCommit = TransactionAfterCommit;
				}

				return tx;
			}
			catch (Exception)
			{
				if (txLockTaken)
					_txWriter.Release();

				throw;
			}
		}

        private void TransactionAfterCommit(long txId)
        {
            Transaction tx;
            _activeTransactions.TryGetValue(txId, out tx);
        }

        internal void TransactionCompleted(long txId)
        {
            Transaction tx;
            if (_activeTransactions.TryRemove(txId, out tx) == false)
                return;

            if (tx.Flags != (TransactionFlags.ReadWrite))
                return;
            try
            {
                if (tx.Committed == false)
                    return;
                _transactionsCounter = txId;
                if (tx.HasModifiedTrees == false)
                    return;
                foreach (var tree in tx.ModifiedTrees)
                {
                    Tree val = tree.Value;
                    if (val == null)
                        _trees.TryRemove(tree.Key, out val);
                    else
                        _trees.AddOrUpdate(tree.Key, val, (s, tree1) => val);
                }
            }
            finally
            {
                _txWriter.Release();
            }
        }

        public void Backup(Stream output)
        {
            Transaction txr = null;
            try
            {
                var buffer = new byte[_dataPager.PageSize*16];
                long nextPageNumber;
                using (var txw = NewTransaction(TransactionFlags.ReadWrite)) // so we can snapshot the headers safely
                {
                    txr = NewTransaction(TransactionFlags.Read); // now have snapshot view
                    nextPageNumber = txw.NextPageNumber;
                    var firstPage = _dataPager.Read(txw, 0);
                    using (var headerStream = new UnmanagedMemoryStream(firstPage.Base, _dataPager.PageSize*2))
                    {
                        while (headerStream.Position < headerStream.Length)
                        {
                            var read = headerStream.Read(buffer, 0, buffer.Length);
                            output.Write(buffer, 0, read);
                        }
                    }
                    //txw.Commit(); intentionally not committing
                }
                // now can copy everything else
                var firtDataPage = _dataPager.Read(txr, 2);
                using (
                    var headerStream = new UnmanagedMemoryStream(firtDataPage.Base, _dataPager.PageSize*(nextPageNumber - 2))
                    )
                {
                    while (headerStream.Position < headerStream.Length)
                    {
                        var read = headerStream.Read(buffer, 0, buffer.Length);
                        output.Write(buffer, 0, read);
                    }
                }
                //txr.Commit(); intentionally not committing
            }
            finally
            {
                if(txr!=null)
                    txr.Dispose();
            }
        }

		private void FlushLogToDataFile(object state)
		{
			if(_activeTransactions.Count(x => x.Value.Flags == TransactionFlags.ReadWrite) > 0)
				return;

			_log.ApplyLogsToDataFile();
		}

        public Dictionary<string, List<long>> AllPages(Transaction tx)
        {
            var results = new Dictionary<string, List<long>>(StringComparer.OrdinalIgnoreCase)
				{
					{"Root", Root.AllPages(tx)},
					{"Free Space Overhead", FreeSpaceHandling.Info.GetBuffersPages()},
					{"Free Pages", FreeSpaceHandling.Info.GetFreePages(tx.Id)}
				};

            foreach (var tree in _trees)
            {
                results.Add(tree.Key, tree.Value.AllPages(tx));
            }

            return results;
        }

		public EnvironmentStats Stats()
		{
			return new EnvironmentStats
				{
					FreePages = FreeSpaceHandling.Info.FreePagesCount,
					FreePagesOverhead = FreeSpaceHandling.Info.GetBuffersPages().Count,
					RootPages = Root.State.PageCount,
					HeaderPages = 2,
					UnallocatedPagesAtEndOfFile = _dataPager.NumberOfAllocatedPages - NextPageNumber
				};
		}
    }
}