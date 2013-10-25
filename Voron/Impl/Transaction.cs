using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Voron.Impl.FileHeaders;
using Voron.Impl.FreeSpace;
using Voron.Impl.Log;
using Voron.Trees;
using Voron.Util;

namespace Voron.Impl
{
	public class Transaction : IDisposable
	{
		public long NextPageNumber;

		private readonly StorageEnvironment _env;
		private readonly WriteAheadLog _log;
		private readonly IVirtualPager _dataPager;
		private readonly long _id;

		private TreeDataInTransaction _rootTreeData;
		private Dictionary<Tuple<Tree, Slice>, Tree> _multiValueTrees;
		private readonly Dictionary<Tree, TreeDataInTransaction> _treesInfo = new Dictionary<Tree, TreeDataInTransaction>();
		private readonly HashSet<long> _dirtyPages = new HashSet<long>();
		private readonly List<long> _freedPages = new List<long>();
		private readonly HashSet<PagerState> _pagerStates = new HashSet<PagerState>();
		internal readonly List<LogSnapshot> LogSnapshots = new List<LogSnapshot>();
		private readonly List<Action> _releaseLogActions = new List<Action>(); 
		private readonly UnmanagedBits _freeSpaceBuffer;

		public TransactionFlags Flags { get; private set; }

		public StorageEnvironment Environment
		{
			get { return _env; }
		}

		public long Id
		{
			get { return _id; }
		}

		internal Action<long> AfterCommit = delegate { };
		private Dictionary<string, Tree> modifiedTrees;

		public Page TempPage
		{
			get { return _dataPager.TempPage; }
		}

		public Dictionary<string, Tree> ModifiedTrees
		{
			get { return modifiedTrees ?? (modifiedTrees = new Dictionary<string, Tree>(StringComparer.OrdinalIgnoreCase)); }
		}

		public bool Committed { get; private set; }

		public bool HasModifiedTrees
		{
			get { return modifiedTrees != null; }
		}

		public UnmanagedBits FreeSpaceBuffer
		{
			get { return _freeSpaceBuffer; }
		}

		public PagerInfo PagerInfo { get; private set; }

		public Transaction(WriteAheadLog log, IVirtualPager dataPager, StorageEnvironment env, long id, TransactionFlags flags, UnmanagedBits freeSpaceBuffer)
		{
			_log = log;
			_dataPager = dataPager;
			_env = env;
			_id = id;
			_freeSpaceBuffer = freeSpaceBuffer;
			Flags = flags;
			NextPageNumber = env.NextPageNumber;
			PagerInfo = new PagerInfo()
				{
					MaxNodeSize = _dataPager.MaxNodeSize,
					PageMaxSpace = _dataPager.PageMaxSpace,
					PageMinSpace = _dataPager.PageMinSpace,
					PageSize = _dataPager.PageSize
				};

			if (flags == TransactionFlags.ReadWrite)
			{
				_log.Files.ForEach(SetLogReference);
				_log.TransactionBegin(this);
			}
			else
			{
				_log.GetSnapshots().ForEach(AddLogSnapshot);
			}

			foreach (var tree in env.Trees)
			{
				GetTreeInformation(tree);
			}
		}

		public unsafe Page ModifyPage(long p, Cursor c)
		{
			Page page;
			if (_dirtyPages.Contains(p))
			{
                page = c.GetPage(p) ?? _log.ReadPage(this, p);
                page.Dirty = true;
				
                return page;
			}

			page = c.GetPage(p) ?? _log.ReadPage(this, p) ?? _dataPager.Read(p);

			var newPage = AllocatePage(1, p); // allocate new page in a log file but with the same number
			
			NativeMethods.memcpy(newPage.Base, page.Base, _dataPager.PageSize);
			newPage.LastSearchPosition = page.LastSearchPosition;
			newPage.LastMatch = page.LastMatch;

			return newPage;
		}

		public Page GetReadOnlyPage(long n)
		{
			return _log.ReadPage(this, n) ?? _dataPager.Read(n);
		}

		private long? TryAllocateFromFreeSpace(int numberOfPages)
		{
			if (_freeSpaceBuffer == null)
				return null;

			var page = _freeSpaceBuffer.Find(numberOfPages);

			if (page == -1)
				return null;

		    return page;
		}

		public Page AllocatePage(int numberOfPages, long? pageNumber = null)
		{
			if (pageNumber == null)
			{
				pageNumber = TryAllocateFromFreeSpace(numberOfPages);
				if (pageNumber == null) // allocate from end of file
				{
					pageNumber = NextPageNumber;
					NextPageNumber += numberOfPages;
				}
			}

			var page = _log.Allocate(this, pageNumber.Value, numberOfPages);

			page.PageNumber = pageNumber.Value;
			page.Lower = (ushort) Constants.PageHeaderSize;
			page.Upper = (ushort) _dataPager.PageSize;
			page.Dirty = true;

		    _dirtyPages.Add(page.PageNumber);
			return page;
		}


		internal unsafe int GetNumberOfFreePages(NodeHeader* node)
		{
			return GetNodeDataSize(node) / Constants.PageNumberSize;
		}

		internal unsafe int GetNodeDataSize(NodeHeader* node)
		{
			if (node->Flags == (NodeFlags.PageRef)) // lots of data, enough to overflow!
			{
				var overflowPage = GetReadOnlyPage(node->PageNumber);
				return overflowPage.OverflowSize;
			}
			return node->DataSize;
		}

		public unsafe void Commit()
		{
			if (Flags != (TransactionFlags.ReadWrite))
				return; // nothing to do

			FlushAllMultiValues();

			foreach (var kvp in _treesInfo)
			{
				var txInfo = kvp.Value;
				var tree = kvp.Key;

				if (false && // TODO txInfo.RootPageNumber == tree.State.RootPageNumber && - now we can make a change without changing it's page number
				    (modifiedTrees == null || modifiedTrees.ContainsKey(tree.Name) == false))
					continue; // not modified

				tree.DebugValidateTree(this, txInfo.RootPageNumber);
				txInfo.Flush();
				if (string.IsNullOrEmpty(kvp.Key.Name))
					continue;
		
				var treePtr = (TreeRootHeader*)_env.Root.DirectAdd(this, tree.Name, sizeof(TreeRootHeader));
				tree.State.CopyTo(treePtr);
			}

			if (_freedPages.Count > 0)
				_env.FreeSpaceHandling.RegisterFreePages(this, _freedPages);   // this is the the free space that is available when all concurrent transactions are done

			if (_rootTreeData != null)
			{
				_env.Root.DebugValidateTree(this, _rootTreeData.RootPageNumber);
				_rootTreeData.Flush();
			}

			_env.NextPageNumber = NextPageNumber;

			List<long> dirtyFreeSpacePages = null;
			if (_freeSpaceBuffer != null)
			{
				_env.FreeSpaceHandling.OnTransactionCommit(_freeSpaceBuffer, _env.OldestTransaction, out dirtyFreeSpacePages);
				_env.FreeSpaceHandling.UpdateChecksum(_freeSpaceBuffer.CalculateChecksum());
			}

			_log.TransactionCommit(this);

			Committed = true;

			AfterCommit(_id);
		}

		private unsafe void FlushAllMultiValues()
		{
			if (_multiValueTrees == null)
				return;

			foreach (var multiValueTree in _multiValueTrees)
			{
				var parentTree = multiValueTree.Key.Item1;
				var key = multiValueTree.Key.Item2;
				var childTree = multiValueTree.Value;

				TreeDataInTransaction value;
				if (_treesInfo.TryGetValue(childTree, out value) == false)
					continue;

				_treesInfo.Remove(childTree);
				var trh = (TreeRootHeader*)parentTree.DirectAdd(this, key, sizeof(TreeRootHeader));
				value.State.CopyTo(trh);

				parentTree.SetAsMultiValueTreeRef(this, key);
			}
		}

		public void Dispose()
		{
			_env.TransactionCompleted(_id);
			foreach (var pagerState in _pagerStates)
			{
				pagerState.Release();
			}

			foreach (var releaseLog in _releaseLogActions)
			{
				releaseLog();
			}
		}

		public TreeDataInTransaction GetTreeInformation(Tree tree)
		{
			// ReSharper disable once PossibleUnintendedReferenceComparison
			if (tree == _env.Root)
			{
				return _rootTreeData ?? (_rootTreeData = new TreeDataInTransaction(_env.Root)
					{
						RootPageNumber = _env.Root.State.RootPageNumber
					});
			}

			TreeDataInTransaction c;
			if (_treesInfo.TryGetValue(tree, out c))
			{
				return c;
			}
			c = new TreeDataInTransaction(tree)
				{
					RootPageNumber = tree.State.RootPageNumber
				};
			_treesInfo.Add(tree, c);
			return c;
		}

		public void FreePage(long pageNumber)
		{
			_dirtyPages.Remove(pageNumber);

			Debug.Assert(pageNumber >= 2);
			Debug.Assert(_freedPages.Contains(pageNumber) == false);

			_freedPages.Add(pageNumber);
		}

		internal void UpdateRoot(Tree root)
		{
			if (_treesInfo.TryGetValue(root, out _rootTreeData))
			{
				_treesInfo.Remove(root);
			}
			else
			{
				_rootTreeData = new TreeDataInTransaction(root);
			}
		}

		public void AddPagerState(PagerState state)
		{
			_pagerStates.Add(state);
		}

		public Cursor NewCursor(Tree tree)
		{
			return new Cursor();
		}

		public unsafe void AddMultiValueTree(Tree tree, Slice key, Tree mvTree)
		{
			if (_multiValueTrees == null)
				_multiValueTrees = new Dictionary<Tuple<Tree, Slice>, Tree>(new TreeAndSliceComparer(_env.SliceComparer));
			mvTree.IsMultiValueTree = true;
			_multiValueTrees.Add(Tuple.Create(tree, key), mvTree);
		}

		public bool TryGetMultiValueTree(Tree tree, Slice key, out Tree mvTree)
		{
			mvTree = null;
			if (_multiValueTrees == null)
				return false;
			return _multiValueTrees.TryGetValue(Tuple.Create(tree, key), out mvTree);
		}

		public bool TryRemoveMultiValueTree(Tree parentTree, Slice key)
		{
			var keyToRemove = Tuple.Create(parentTree, key);
			if (_multiValueTrees == null || !_multiValueTrees.ContainsKey(keyToRemove))
				return false;

			return _multiValueTrees.Remove(keyToRemove);
		}

		private void AddLogSnapshot(LogSnapshot snapshot)
		{
			if (LogSnapshots.Any(x => x.File.Number == snapshot.File.Number))
				throw new InvalidOperationException("Cannot add a snapshot of log file with number " + snapshot.File.Number +
				                                    " to the transaction, because it already exists in a snapshot collection");

			LogSnapshots.Add(snapshot);
			SetLogReference(snapshot.File);
		}

		public void SetLogReference(LogFile log)
		{
			log.AddRef();
			_releaseLogActions.Add(log.Release);
		}
	}

	internal unsafe class TreeAndSliceComparer : IEqualityComparer<Tuple<Tree, Slice>>
	{
		private readonly SliceComparer _comparer;

		public TreeAndSliceComparer(SliceComparer comparer)
		{
			_comparer = comparer;
		}

		public bool Equals(Tuple<Tree, Slice> x, Tuple<Tree, Slice> y)
		{
			if (x == null && y == null)
				return true;
			if (x == null || y == null)
				return false;

			if (x.Item1 != y.Item1)
				return false;

			return x.Item2.Compare(y.Item2, _comparer) == 0;
		}

		public int GetHashCode(Tuple<Tree, Slice> obj)
		{
			return obj.Item1.GetHashCode() ^ 397 * obj.Item2.GetHashCode();
		}
	}
}