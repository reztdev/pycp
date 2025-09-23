#!/usr/bin/env python3
"""
pycp.py - safer, feature-rich copy tool in pure Python (stdlib only)

Features:
- recursive copy (-r)
- preserve metadata (-p)
- follow symlinks (-L) or copy symlink as link (--no-dereference)
- sparse detection and hole-creation
- atomic writes via temp file + os.replace
- no-clobber (-n), update-only (-u)
- best-effort xattr copy when available
- verbose & dry-run
"""

import os
import sys
import argparse
import shutil
import tempfile
import stat
import hashlib
import errno
import signal
import time
from pathlib import Path

# Constants
DEFAULT_BUF = 64 * 1024
SPARSE_ZERO_BLOCK = 4096  # treat blocks of zeros >= this as sparse candidate

# Global to track tempfiles for cleanup on signal
_tempfiles = set()

def sig_handler(signum, frame):
    for tf in list(_tempfiles):
        try:
            if os.path.exists(tf):
                os.remove(tf)
        except Exception:
            pass
    sys.exit(1)

signal.signal(signal.SIGINT, sig_handler)
signal.signal(signal.SIGTERM, sig_handler)

def is_all_zero(b: bytes) -> bool:
    # fast check
    return b.count(b'\x00') == len(b)

def copy_xattrs(src, dst, follow_symlinks):
    # best-effort copy of extended attributes (Linux, macOS). Not critical if fails.
    try:
        if hasattr(os, 'listxattr') and hasattr(os, 'getxattr') and hasattr(os, 'setxattr'):
            flags = 0 if follow_symlinks else os.XATTR_NOFOLLOW if hasattr(os, 'XATTR_NOFOLLOW') else 0
            try:
                attrs = os.listxattr(src, follow_symlinks=follow_symlinks)
            except TypeError:
                attrs = os.listxattr(src)
            for a in attrs:
                try:
                    val = os.getxattr(src, a, follow_symlinks=follow_symlinks)
                    try:
                        os.setxattr(dst, a, val, follow_symlinks=follow_symlinks)
                    except TypeError:
                        os.setxattr(dst, a, val)
                except Exception:
                    # skip unreadable/unsupported attrs
                    continue
    except Exception:
        pass

def preserve_metadata(src, dst, follow_symlinks):
    # copy permission bits, timestamps, flags (best-effort)
    try:
        shutil.copystat(src, dst, follow_symlinks=follow_symlinks)
    except Exception:
        try:
            shutil.copystat(src, dst)
        except Exception:
            pass
    # try copy owner if running as root
    try:
        st = os.stat(src, follow_symlinks=follow_symlinks)
        try:
            os.chown(dst, st.st_uid, st.st_gid, follow_symlinks=follow_symlinks)
        except TypeError:
            os.chown(dst, st.st_uid, st.st_gid)
    except PermissionError:
        # not privileged to chown => ignore
        pass
    except Exception:
        pass
    # xattr
    copy_xattrs(src, dst, follow_symlinks)

def atomic_tempfile_in_dir(dest_dir, prefix=".pycp_tmp"):
    fd, tmp = tempfile.mkstemp(prefix=prefix, dir=dest_dir)
    os.close(fd)
    _tempfiles.add(tmp)
    return tmp

def write_buffered_with_sparse(src_path, dst_tmp_path, bufsize=DEFAULT_BUF, sparse_threshold=SPARSE_ZERO_BLOCK, verbose=False):
    """
    Read src_path and write to dst_tmp_path using buffers.
    If chunk is all zeros and >= sparse_threshold, perform os.lseek to create hole.
    """
    with open(src_path, 'rb') as fin, open(dst_tmp_path, 'r+b') as fout:
        while True:
            chunk = fin.read(bufsize)
            if not chunk:
                break
            if len(chunk) >= sparse_threshold and is_all_zero(chunk):
                # create hole by moving file pointer
                # move forward by len(chunk) bytes; subsequent write will create hole
                try:
                    cur = fout.tell()
                    os.lseek(fout.fileno(), len(chunk), os.SEEK_CUR)
                except OSError:
                    # fallback to writing zeros (might be slow)
                    fout.write(chunk)
                if verbose:
                    print(f"  [sparse] created hole of {len(chunk)} bytes at offset {cur}")
            else:
                fout.write(chunk)

def copy_file(src, dst, *, preserve=False, follow_symlinks=False, bufsize=DEFAULT_BUF, sparse_threshold=SPARSE_ZERO_BLOCK, atomic=True, verbose=False):
    """
    Copy a single file from src to dst.
    - If atomic, write to temp in dest dir, then os.replace.
    - follow_symlinks: if True, copy target; else copy symlink itself (handled outside)
    """
    src_path = Path(src)
    dst_path = Path(dst)

    if not src_path.exists():
        raise FileNotFoundError(src)

    if src_path.is_symlink() and not follow_symlinks:
        # create symlink at dst with same target
        target = os.readlink(src)
        if dst_path.exists():
            dst_path.unlink()
        os.symlink(target, dst)
        if preserve:
            try:
                preserve_metadata(src, dst, follow_symlinks=False)
            except Exception:
                pass
        return

    # ensure destination directory exists
    dst_dir = dst_path.parent
    dst_dir.mkdir(parents=True, exist_ok=True)

    # decide temp file
    if atomic:
        tmp = atomic_tempfile_in_dir(str(dst_dir))
        # open tmp with size 0 for r+b in writer
        with open(tmp, 'wb') as _:
            pass
        # re-open with r+b in writer function
        try:
            # attempt fast path using os.sendfile if available and file descriptors are regular files
            used_sendfile = False
            try:
                if hasattr(os, 'sendfile'):
                    with open(src, 'rb') as fsrc, open(tmp, 'r+b') as fdst:
                        # sendfile may not accept same-size counts on all platforms; copy in loop
                        offset = 0
                        while True:
                            chunk_sz = os.sendfile(fdst.fileno(), fsrc.fileno(), offset, bufsize)
                            if chunk_sz == 0:
                                break
                            offset += chunk_sz
                    used_sendfile = True
            except Exception:
                used_sendfile = False

            if not used_sendfile:
                # open tmp for binary read/write
                with open(tmp, 'r+b') as fdst:
                    write_buffered_with_sparse(src, tmp, bufsize=bufsize, sparse_threshold=sparse_threshold, verbose=verbose)
        except Exception as e:
            # cleanup tmp and re-raise
            try:
                os.remove(tmp)
            except Exception:
                pass
            _tempfiles.discard(tmp)
            raise

        # replace into final location atomically
        try:
            os.replace(tmp, str(dst_path))
            _tempfiles.discard(tmp)
        except Exception as e:
            # fallback to move
            try:
                shutil.move(tmp, str(dst_path))
                _tempfiles.discard(tmp)
            except Exception:
                _tempfiles.discard(tmp)
                raise
    else:
        # non-atomic direct copy
        with open(src, 'rb') as fs, open(dst, 'wb') as fd:
            while True:
                data = fs.read(bufsize)
                if not data:
                    break
                fd.write(data)

    # finally preserve metadata if requested
    if preserve:
        try:
            preserve_metadata(src, dst, follow_symlinks=follow_symlinks)
        except Exception:
            pass

def should_skip_copy(src, dst, no_clobber=False, update_only=False):
    if not os.path.exists(dst):
        return False
    if no_clobber:
        return True
    if update_only:
        try:
            s_m = os.path.getmtime(src)
            d_m = os.path.getmtime(dst)
            return s_m <= d_m
        except Exception:
            return False
    return False

def copy_tree(src_root, dst_root, *, recursive=True, preserve=False, follow_symlinks=False,
              bufsize=DEFAULT_BUF, sparse_threshold=SPARSE_ZERO_BLOCK, atomic=True, verbose=False,
              no_clobber=False, update_only=False):
    """
    Recursively copy tree src_root -> dst_root. Mirrors behavior of cp -r with options.
    """
    src_root = os.path.abspath(src_root)
    dst_root = os.path.abspath(dst_root)

    if not os.path.exists(src_root):
        raise FileNotFoundError(src_root)

    if os.path.isfile(src_root) or os.path.islink(src_root):
        # single file copy
        if os.path.isdir(dst_root):
            dst = os.path.join(dst_root, os.path.basename(src_root))
        else:
            dst = dst_root
        if should_skip_copy(src_root, dst, no_clobber=no_clobber, update_only=update_only):
            if verbose:
                print(f"[skip] {src_root} -> {dst}")
            return
        copy_file(src_root, dst, preserve=preserve, follow_symlinks=follow_symlinks, bufsize=bufsize,
                  sparse_threshold=sparse_threshold, atomic=atomic, verbose=verbose)
        if verbose:
            print(f"[ok] {src_root} -> {dst}")
        return

    # directory copy
    if not recursive:
        raise IsADirectoryError(f"{src_root} is a directory (use -r to copy recursively)")

    # Walk src tree
    for root, dirs, files in os.walk(src_root, followlinks=follow_symlinks):
        rel = os.path.relpath(root, src_root)
        dst_dir = os.path.join(dst_root, rel) if rel != '.' else dst_root
        os.makedirs(dst_dir, exist_ok=True)
        # preserve metadata for directories if requested
        if preserve:
            try:
                preserve_metadata(root, dst_dir, follow_symlinks=follow_symlinks)
            except Exception:
                pass
        for fname in files:
            s = os.path.join(root, fname)
            d = os.path.join(dst_dir, fname)
            if should_skip_copy(s, d, no_clobber=no_clobber, update_only=update_only):
                if verbose:
                    print(f"[skip] {s} -> {d}")
                continue
            try:
                copy_file(s, d, preserve=preserve, follow_symlinks=follow_symlinks,
                          bufsize=bufsize, sparse_threshold=sparse_threshold, atomic=atomic, verbose=verbose)
                if verbose:
                    print(f"[ok] {s} -> {d}")
            except Exception as e:
                print(f"[error] failed to copy {s} -> {d}: {e}", file=sys.stderr)

        # handle directories symlinked if any
        for dname in dirs:
            sdir = os.path.join(root, dname)
            ddir = os.path.join(dst_dir, dname)
            if os.path.islink(sdir) and not follow_symlinks:
                # replicate symlink
                try:
                    target = os.readlink(sdir)
                    if os.path.exists(ddir):
                        os.remove(ddir)
                    os.symlink(target, ddir)
                except Exception:
                    pass

def parse_args(argv):
    p = argparse.ArgumentParser(description="pycp - safer cp-like tool (Python stdlib)")
    p.add_argument("src", nargs="+", help="Source file(s) or directory")
    p.add_argument("dst", help="Destination file or directory")
    p.add_argument("-r", "--recursive", action="store_true", help="Copy directories recursively")
    p.add_argument("-p", "--preserve", action="store_true", help="Preserve metadata (mode, timestamps, ownership where possible)")
    p.add_argument("-L", "--dereference", action="store_true", help="Follow symlinks (copy target files)")
    p.add_argument("-n", "--no-clobber", action="store_true", help="Do not overwrite existing files")
    p.add_argument("-u", "--update", action="store_true", help="Copy only when source is newer than destination")
    p.add_argument("--sparse-threshold", type=int, default=SPARSE_ZERO_BLOCK, help="Threshold for zero-block detection to create sparse hole")
    p.add_argument("--bufsize", type=int, default=DEFAULT_BUF, help="Buffer size for copy (bytes)")
    p.add_argument("--no-atomic", action="store_true", help="Do not use atomic temp file + replace (use direct write)")
    p.add_argument("-v", "--verbose", action="store_true", help="Verbose output")
    p.add_argument("--dry-run", action="store_true", help="Print actions but do not perform writes")
    return p.parse_args(argv)

def main(argv):
    args = parse_args(argv)
    srcs = args.src
    dst = args.dst

    # Multisource -> dst must be directory or existing directory
    if len(srcs) > 1:
        if not os.path.isdir(dst):
            print("When copying multiple sources, destination must be a directory", file=sys.stderr)
            sys.exit(2)

    for s in srcs:
        s_abs = os.path.abspath(s)
        # determine final destination path
        if os.path.isdir(dst):
            d_final = os.path.join(dst, os.path.basename(s))
        else:
            d_final = dst

        if args.dry_run:
            print(f"[dry-run] would copy {s_abs} -> {d_final}")
            continue

        try:
            copy_tree(s, d_final, recursive=args.recursive, preserve=args.preserve,
                      follow_symlinks=args.dereference, bufsize=args.bufsize,
                      sparse_threshold=args.sparse_threshold, atomic=(not args.no_atomic),
                      verbose=args.verbose, no_clobber=args.no_clobber, update_only=args.update)
        except Exception as e:
            print(f"Error copying {s} -> {d_final}: {e}", file=sys.stderr)

if __name__ == "__main__":
    main(sys.argv[1:])
