using System;

namespace Voron.Platform.Posix
{
    public struct Stat
        : IEquatable<Stat>
    {
        [CLSCompliant(false)]
        public ulong st_dev;     // device
        [CLSCompliant(false)]
        public ulong st_ino;     // inode
        [CLSCompliant(false)]
        public FilePermissions st_mode;    // protection
        [NonSerialized]
#pragma warning disable 169
            private uint _padding_;  // padding for structure alignment
#pragma warning restore 169
        [CLSCompliant(false)]
        public ulong st_nlink;   // number of hard links
        [CLSCompliant(false)]
        public uint st_uid;     // user ID of owner
        [CLSCompliant(false)]
        public uint st_gid;     // group ID of owner
        [CLSCompliant(false)]
        public ulong st_rdev;    // device type (if inode device)
        public long st_size;    // total size, in bytes
        public long st_blksize; // blocksize for filesystem I/O
        public long st_blocks;  // number of blocks allocated
        public long st_atime;   // time of last access
        public long st_mtime;   // time of last modification
        public long st_ctime;   // time of last status change
        public long st_atime_nsec; // Timespec.tv_nsec partner to st_atime
        public long st_mtime_nsec; // Timespec.tv_nsec partner to st_mtime
        public long st_ctime_nsec; // Timespec.tv_nsec partner to st_ctime

        public override int GetHashCode()
        {
            return st_dev.GetHashCode() ^
                   st_ino.GetHashCode() ^
                   st_mode.GetHashCode() ^
                   st_nlink.GetHashCode() ^
                   st_uid.GetHashCode() ^
                   st_gid.GetHashCode() ^
                   st_rdev.GetHashCode() ^
                   st_size.GetHashCode() ^
                   st_blksize.GetHashCode() ^
                   st_blocks.GetHashCode() ^
                   st_atime.GetHashCode() ^
                   st_mtime.GetHashCode() ^
                   st_ctime.GetHashCode() ^
                   st_atime_nsec.GetHashCode() ^
                   st_mtime_nsec.GetHashCode() ^
                   st_ctime_nsec.GetHashCode();
        }

        public override bool Equals(object obj)
        {
            if (obj == null || obj.GetType() != GetType())
                return false;
            Stat value = (Stat)obj;
            return value.st_dev == st_dev &&
                   value.st_ino == st_ino &&
                   value.st_mode == st_mode &&
                   value.st_nlink == st_nlink &&
                   value.st_uid == st_uid &&
                   value.st_gid == st_gid &&
                   value.st_rdev == st_rdev &&
                   value.st_size == st_size &&
                   value.st_blksize == st_blksize &&
                   value.st_blocks == st_blocks &&
                   value.st_atime == st_atime &&
                   value.st_mtime == st_mtime &&
                   value.st_ctime == st_ctime &&
                   value.st_atime_nsec == st_atime_nsec &&
                   value.st_mtime_nsec == st_mtime_nsec &&
                   value.st_ctime_nsec == st_ctime_nsec;
        }

        public bool Equals(Stat value)
        {
            return value.st_dev == st_dev &&
                   value.st_ino == st_ino &&
                   value.st_mode == st_mode &&
                   value.st_nlink == st_nlink &&
                   value.st_uid == st_uid &&
                   value.st_gid == st_gid &&
                   value.st_rdev == st_rdev &&
                   value.st_size == st_size &&
                   value.st_blksize == st_blksize &&
                   value.st_blocks == st_blocks &&
                   value.st_atime == st_atime &&
                   value.st_mtime == st_mtime &&
                   value.st_ctime == st_ctime &&
                   value.st_atime_nsec == st_atime_nsec &&
                   value.st_mtime_nsec == st_mtime_nsec &&
                   value.st_ctime_nsec == st_ctime_nsec;
        }

        public static bool operator ==(Stat lhs, Stat rhs)
        {
            return lhs.Equals(rhs);
        }

        public static bool operator !=(Stat lhs, Stat rhs)
        {
            return !lhs.Equals(rhs);
        }
    }
}