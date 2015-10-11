using Voron.Headers;
using Voron.Journal;
using Voron.Pagers;

namespace Voron.Impl
{
    public class PureMemoryStorageEnvironmentOptions : StorageEnvironmentOptions
    {
        public override IVirtualPager DataPager
        {
            get { throw new System.NotImplementedException(); }
        }

        public override string BasePath
        {
            get { throw new System.NotImplementedException(); }
        }

        public override IJournalWriter CreateJournalWriter(long journalNumber, long journalSize)
        {
            throw new System.NotImplementedException();
        }

        public override void Dispose()
        {
            throw new System.NotImplementedException();
        }

        public override bool TryDeleteJournal(long number)
        {
            throw new System.NotImplementedException();
        }

        public override unsafe bool ReadHeader(string filename, FileHeader* header)
        {
            throw new System.NotImplementedException();
        }

        public override unsafe void WriteHeader(string filename, FileHeader* header)
        {
            throw new System.NotImplementedException();
        }

        public override IVirtualPager CreateScratchPager(string name)
        {
            throw new System.NotImplementedException();
        }

        public override IVirtualPager OpenJournalPager(long journalNumber)
        {
            throw new System.NotImplementedException();
        }

        public override IVirtualPager OpenPager(string filename)
        {
            throw new System.NotImplementedException();
        }
    }
}