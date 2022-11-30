#include <rocksdb/enum_reflection.h>
#include <sys/mount.h>
#include <sys/stat.h>
#include <sys/types.h>

ROCKSDB_ENUM_CLASS(MountFlags, size_t,
  dirsync     = MS_DIRSYNC,
  i_version   = MS_I_VERSION,
  lazytime    = MS_LAZYTIME,
  mandlock    = MS_MANDLOCK,
  noatime     = MS_NOATIME,
  nodev       = MS_NODEV,
  nodiratime  = MS_NODIRATIME,
  noexec      = MS_NOEXEC,
  nosuid      = MS_NOSUID,
  nouser      = size_t(MS_NOUSER),
  posixacl    = MS_POSIXACL,
  Private     = MS_PRIVATE,
  rec         = MS_REC,
  relatime    = MS_RELATIME,
  remount     = MS_REMOUNT,
  rmt_mask    = MS_RMT_MASK,
  shared      = MS_SHARED,
  silent      = MS_SILENT,
  slave       = MS_SLAVE,
  strictatime = MS_STRICTATIME,
  synchronous = MS_SYNCHRONOUS,
  unbindable  = MS_UNBINDABLE,
//nosymfollow = MS_NOSYMFOLLOW,
  readonly    = MS_RDONLY,
  rdonly      = MS_RDONLY
);
