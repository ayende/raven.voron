using System.Runtime.InteropServices;

namespace Voron.Headers
{
    [StructLayout(LayoutKind.Explicit, Pack = 1)]
    public struct FileHeader
    {
        /// <summary>
        /// Just a value chosen to mark our files headers, this is used to 
        /// make sure that we are opening the right format file
        /// </summary>
        [FieldOffset(0)]
        public ulong MagicMarker;
        /// <summary>
        /// The version of the data, used for versioning / conflicts
        /// </summary>
        [FieldOffset(8)]
        public int Version;

		/// <summary>
		/// Incremented on every header modification
		/// </summary>
		[FieldOffset(12)]
		public long HeaderRevision;

        /// <summary>
        /// The transaction id that committed this page
        /// </summary>
        [FieldOffset(20)]
        public long TransactionId;

        /// <summary>
        /// The last used page number for this file
        /// </summary>
        [FieldOffset(28)]
        public long LastPageNumber;

        /// <summary>
        /// Information about the journal log info
        /// </summary>
        [FieldOffset(36)] 
        public JournalInfo Journal;

		/// <summary>
		/// Information about an incremental backup
		/// </summary>
	    [FieldOffset(64)] 
		public IncrementalBackupInfo IncrementalBackup;

		/// <summary>
		/// Crc of last shipped transaction
		/// </summary>
		[FieldOffset(88)]
		public uint PreviousTransactionCrc;

        /// <summary>
        /// The (variable size) tree for all the root pages
        /// </summary>
        [FieldOffset(92)] 
        public TreeRootHeader RootObjects;

        /// <summary>
        /// The (fixed size) tree for all the free space
        /// </summary>
        [FieldOffset(120)]
        public TreeRootHeader FreeSpace;


        [StructLayout(LayoutKind.Explicit, Pack = 1)]
        public struct IncrementalBackupInfo
        {
            [FieldOffset(0)]
            public long LastBackedUpJournal;

            [FieldOffset(8)]
            public long LastBackedUpJournalPage;

            [FieldOffset(16)]
            public long LastCreatedJournal;
        }

        [StructLayout(LayoutKind.Explicit, Pack = 1)]
        public struct JournalInfo
        {
            [FieldOffset(0)]
            public long CurrentJournal;

            [FieldOffset(8)]
            public int JournalFilesCount;

            [FieldOffset(12)]
            public long LastSyncedJournal;

            [FieldOffset(20)]
            public long LastSyncedTransactionId;
        }
	}
}