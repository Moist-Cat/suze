#!/usr/bin/env python3
"""
Cache Inspector CLI Tool
For inspecting and managing FileCache entries in a cold environment.
"""

import json
import pickle
import time
from pathlib import Path
from typing import Dict, List, Any, Optional
import sys
import textwrap
from datetime import datetime
import argparse
import readline  # For better input handling


class CacheInspector:
    """Tool for inspecting and managing FileCache entries."""

    def __init__(self, cache_dir: Path, metadata_path: Optional[Path] = None):
        self.cache_dir = Path(cache_dir)
        self.meta_path = metadata_path or self.cache_dir / ".meta"

        if not self.cache_dir.exists():
            raise ValueError(f"Cache directory not found: {cache_dir}")

        self.metadata = self._load_metadata()
        self._cache_files = self._discover_cache_files()

    def _load_metadata(self) -> Dict[str, Any]:
        """Load metadata from .meta file."""
        if not self.meta_path.exists():
            print(
                f"Warning: Metadata file not found at {self.meta_path}", file=sys.stderr
            )
            return {"access": {}, "ttl": {}}

        try:
            with open(self.meta_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError) as e:
            print(f"Error loading metadata: {e}", file=sys.stderr)
            return {"access": {}, "ttl": {}}

    def _discover_cache_files(self) -> Dict[int, Path]:
        """Discover all cache files in the directory."""
        cache_files = {}
        for file_path in self.cache_dir.glob("*.cache"):
            try:
                key_str = file_path.stem
                key = int(key_str)
                cache_files[key] = file_path
            except ValueError:
                print(
                    f"Warning: Invalid cache file name: {file_path.name}",
                    file=sys.stderr,
                )
        return cache_files

    def _get_cache_path(self, key: int) -> Path:
        """Get path for a cache key."""
        return self.cache_dir / f"{key}.cache"

    def _read_cache_file(self, key: int) -> Any:
        """Read and unpickle a cache file."""
        cache_path = self._get_cache_path(key)

        if not cache_path.exists():
            raise FileNotFoundError(f"Cache file not found for key: {key}")

        try:
            with open(cache_path, "rb") as f:
                return pickle.load(f)
        except (pickle.PickleError, EOFError, OSError) as e:
            raise ValueError(f"Failed to unpickle cache file for key {key}: {e}")

    def list_entries(self, show_expired: bool = False) -> List[Dict[str, Any]]:
        """List all cache entries with metadata."""
        entries = []
        now = time.time()

        for key, cache_path in self._cache_files.items():
            # Get metadata
            access_count = self.metadata.get("access", {}).get(str(key), 0)
            ttl_timestamp = self.metadata.get("ttl", {}).get(str(key))

            # Determine status
            status = "VALID"
            is_expired = False

            if ttl_timestamp:
                if now > ttl_timestamp:
                    status = "EXPIRED"
                    is_expired = True
                else:
                    expires_in = ttl_timestamp - now
                    days = int(expires_in // (24 * 3600))
                    hours = int((expires_in % (24 * 3600)) // 3600)
                    status = f"VALID ({days}d {hours}h)"

            # Skip expired if not requested
            if not show_expired and is_expired:
                continue

            # Get file info
            try:
                file_size = cache_path.stat().st_size
                mtime = datetime.fromtimestamp(cache_path.stat().st_mtime)
            except OSError:
                file_size = 0
                mtime = None

            entries.append(
                {
                    "key": key,
                    "size": file_size,
                    "access_count": access_count,
                    "status": status,
                    "expires_at": (
                        datetime.fromtimestamp(ttl_timestamp) if ttl_timestamp else None
                    ),
                    "modified": mtime,
                    "is_expired": is_expired,
                    "has_metadata": str(key) in self.metadata.get("ttl", {}),
                }
            )

        return entries

    def inspect_entry(self, key: int, max_preview: int = 200) -> Dict[str, Any]:
        """Inspect a specific cache entry."""
        result = {
            "key": key,
            "exists": False,
            "valid": False,
            "data": None,
            "metadata": {},
            "error": None,
        }

        # Check if cache file exists
        cache_path = self._get_cache_path(key)
        if not cache_path.exists():
            result["error"] = f"Cache file not found for key: {key}"
            return result

        result["exists"] = True

        # Get metadata
        access_count = self.metadata.get("access", {}).get(str(key), 0)
        ttl_timestamp = self.metadata.get("ttl", {}).get(str(key))

        result["metadata"] = {
            "access_count": access_count,
            "ttl_timestamp": ttl_timestamp,
            "expires_at": (
                datetime.fromtimestamp(ttl_timestamp) if ttl_timestamp else None
            ),
            "is_expired": ttl_timestamp and time.time() > ttl_timestamp,
            "has_ttl": ttl_timestamp is not None,
        }

        # Try to read cache file
        try:
            data = self._read_cache_file(key)
            result["valid"] = True
            result["data_type"] = type(data).__name__

            # Create preview
            if isinstance(data, str):
                preview = data[:max_preview] + (
                    "..." if len(data) > max_preview else ""
                )
            elif isinstance(data, list):
                preview = f"List with {len(data)} items"
                if data and len(data) > 0:
                    preview += f" (first item type: {type(data[0]).__name__})"
            elif isinstance(data, dict):
                preview = f"Dict with {len(data)} keys"
                if data:
                    preview += (
                        f" (keys: {list(data.keys())[:3]}..., value: {str(data)[:100]})"
                        if len(data) > 3
                        else f" (keys: {list(data.keys())}), value: {str(data)[:100]}"
                    )
            else:
                preview = str(data)[:max_preview] + (
                    "..." if len(str(data)) > max_preview else ""
                )

            result["preview"] = preview
            result["size"] = cache_path.stat().st_size

        except ValueError as e:
            result["error"] = str(e)
            result["valid"] = False

        return result

    def delete_entry(self, key: int, delete_metadata: bool = True) -> bool:
        """Delete a cache entry."""
        cache_path = self._get_cache_path(key)

        # Delete cache file
        try:
            if cache_path.exists():
                cache_path.unlink()
                print(f"Deleted cache file: {cache_path}")
            else:
                print(f"Cache file not found: {cache_path}")
        except OSError as e:
            print(f"Error deleting cache file: {e}", file=sys.stderr)
            return False

        # Delete metadata if requested
        if delete_metadata:
            for dict_key in ["access", "ttl"]:
                if dict_key in self.metadata and str(key) in self.metadata[dict_key]:
                    del self.metadata[dict_key][str(key)]

            # Save updated metadata
            self._save_metadata()
            print(f"Removed metadata for key: {key}")

        return True

    def purge_expired(self, dry_run: bool = False) -> Dict[str, Any]:
        """Find and delete all expired entries."""
        now = time.time()
        expired_entries = []
        valid_entries = []

        for key, cache_path in self._cache_files.items():
            ttl_timestamp = self.metadata.get("ttl", {}).get(str(key))

            # Check if expired
            is_expired = ttl_timestamp and now > ttl_timestamp

            # Also check if file is corrupted
            is_corrupted = False
            try:
                self._read_cache_file(key)
            except ValueError:
                is_corrupted = True

            entry_info = {
                "key": key,
                "expired": is_expired,
                "corrupted": is_corrupted,
                "path": cache_path,
                "size": cache_path.stat().st_size if cache_path.exists() else 0,
            }

            if is_expired or is_corrupted:
                expired_entries.append(entry_info)
            else:
                valid_entries.append(entry_info)

        # Report
        total_size = sum(e["size"] for e in expired_entries)

        result = {
            "expired_count": len(expired_entries),
            "valid_count": len(valid_entries),
            "total_size": total_size,
            "entries": expired_entries,
        }

        # Actually delete if not dry run
        if not dry_run:
            deleted_count = 0
            for entry in expired_entries:
                if self.delete_entry(entry["key"], delete_metadata=True):
                    deleted_count += 1

            result["deleted_count"] = deleted_count

        return result

    def _save_metadata(self) -> bool:
        """Save metadata back to file."""
        try:
            with open(self.meta_path, "w", encoding="utf-8") as f:
                json.dump(self.metadata, f, indent=2)
            return True
        except OSError as e:
            print(f"Error saving metadata: {e}", file=sys.stderr)
            return False

    def repair_metadata(self) -> Dict[str, Any]:
        """Repair metadata by syncing with actual cache files."""
        # Get all cache files
        cache_keys = set(self._cache_files.keys())

        # Get keys in metadata
        meta_access_keys = set(int(k) for k in self.metadata.get("access", {}).keys())
        meta_ttl_keys = set(int(k) for k in self.metadata.get("ttl", {}).keys())

        # Find orphans
        orphaned_files = cache_keys - meta_ttl_keys
        orphaned_metadata = (meta_ttl_keys | meta_access_keys) - cache_keys

        # Remove orphaned metadata
        for key in orphaned_metadata:
            for dict_key in ["access", "ttl"]:
                if dict_key in self.metadata and str(key) in self.metadata[dict_key]:
                    del self.metadata[dict_key][str(key)]

        # Add missing metadata for existing files
        for key in orphaned_files:
            if "access" not in self.metadata:
                self.metadata["access"] = {}
            if "ttl" not in self.metadata:
                self.metadata["ttl"] = {}

            self.metadata["access"][str(key)] = 0
            self.metadata["ttl"][str(key)] = time.time() + (
                30 * 24 * 3600
            )  # Default 30 days

        # Save changes
        self._save_metadata()

        return {
            "orphaned_files": len(orphaned_files),
            "orphaned_metadata": len(orphaned_metadata),
            "total_entries": len(cache_keys),
        }


class CLI:
    """Command-line interface for cache inspection."""

    def __init__(self):
        self.parser = argparse.ArgumentParser(
            description="Inspect and manage FileCache entries",
            formatter_class=argparse.RawDescriptionHelpFormatter,
            epilog=textwrap.dedent(
                """
            Examples:
              %(prog)s /path/to/cache list
              %(prog)s /path/to/cache inspect 12345
              %(prog)s /path/to/cache purge --dry-run
              %(prog)s /path/to/cache delete 12345 67890
            """
            ),
        )

        self.parser.add_argument("cache_dir", help="Path to cache directory")

        self.parser.add_argument(
            "command",
            choices=[
                "list",
                "inspect",
                "delete",
                "purge",
                "stats",
                "repair",
                "interactive",
            ],
            help="Command to execute",
        )

        self.parser.add_argument(
            "keys", nargs="*", help="Cache keys (for inspect/delete commands)"
        )

        self.parser.add_argument(
            "--show-expired",
            action="store_true",
            help="Show expired entries in list (default: hide expired)",
        )

        self.parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Show what would be purged without deleting",
        )

        self.parser.add_argument(
            "--metadata", help="Path to metadata file (default: cache_dir/.meta)"
        )

    def run(self, args=None):
        """Run the CLI."""
        if args is None:
            args = self.parser.parse_args()

        try:
            # Initialize inspector
            metadata_path = Path(args.metadata) if args.metadata else None
            inspector = CacheInspector(Path(args.cache_dir), metadata_path)

            # Execute command
            if args.command == "list":
                self._cmd_list(inspector, args.show_expired)
            elif args.command == "inspect":
                self._cmd_inspect(inspector, args.keys)
            elif args.command == "delete":
                self._cmd_delete(inspector, args.keys)
            elif args.command == "purge":
                self._cmd_purge(inspector, args.dry_run)
            elif args.command == "stats":
                self._cmd_stats(inspector)
            elif args.command == "repair":
                self._cmd_repair(inspector)
            elif args.command == "interactive":
                self._interactive_mode(inspector)

        except Exception as e:
            print(f"Error: {e}", file=sys.stderr)
            sys.exit(1)

    def _cmd_list(self, inspector: CacheInspector, show_expired: bool):
        """List all cache entries."""
        entries = inspector.list_entries(show_expired=show_expired)

        if not entries:
            print("No cache entries found.")
            return

        # Print header
        print(
            f"{'Key':<12} {'Size':<10} {'Access':<8} {'Status':<20} {'Expires/Expired':<25}"
        )
        print("=" * 85)

        # Print entries
        for entry in sorted(entries, key=lambda x: x["key"]):
            size_str = self._format_size(entry["size"])
            expires_str = (
                entry["expires_at"].strftime("%Y-%m-%d %H:%M")
                if entry["expires_at"]
                else "No TTL"
            )

            print(
                f"{entry['key']:<12} {size_str:<10} {entry['access_count']:<8} "
                f"{entry['status']:<20} {expires_str:<25}"
            )

        print(f"\nTotal entries: {len(entries)}")

    def _cmd_inspect(self, inspector: CacheInspector, keys: List[str]):
        """Inspect specific cache entries."""
        if not keys:
            print("Error: No keys specified for inspection", file=sys.stderr)
            return

        for key_str in keys:
            try:
                key = int(key_str)
                result = inspector.inspect_entry(key)

                print(f"\n{'='*60}")
                print(f"INSPECTING KEY: {key}")
                print(f"{'='*60}")

                if not result["exists"]:
                    print(f"Entry not found.")
                    continue

                print(f"Status: {'VALID' if result['valid'] else 'CORRUPT/INVALID'}")
                print(f"File size: {self._format_size(result.get('size', 0))}")

                if result["error"]:
                    print(f"Error: {result['error']}")

                # Show metadata
                meta = result["metadata"]
                print(f"\nMetadata:")
                print(f"  Access count: {meta.get('access_count', 0)}")

                if meta.get("expires_at"):
                    status = "EXPIRED" if meta["is_expired"] else "VALID"
                    print(f"  TTL: {meta['expires_at']} ({status})")
                else:
                    print(f"  TTL: Not set")

                # Show data preview
                if result["valid"]:
                    print(f"\nData:")
                    print(f"  Type: {result.get('data_type', 'Unknown')}")
                    print(f"  Preview: {result.get('preview', 'No preview')}")

                print()

            except ValueError:
                print(f"Error: Invalid key format: {key_str}", file=sys.stderr)

    def _cmd_delete(self, inspector: CacheInspector, keys: List[str]):
        """Delete specific cache entries."""
        if not keys:
            print("Error: No keys specified for deletion", file=sys.stderr)
            return

        deleted = []
        failed = []

        for key_str in keys:
            try:
                key = int(key_str)
                if inspector.delete_entry(key):
                    deleted.append(key_str)
                else:
                    failed.append(key_str)
            except ValueError:
                print(f"Error: Invalid key format: {key_str}", file=sys.stderr)
                failed.append(key_str)

        # Summary
        print(f"\nDelete Summary:")
        print(f"  Successfully deleted: {len(deleted)} entries")
        if deleted:
            print(f"    Keys: {', '.join(deleted)}")
        if failed:
            print(f"  Failed to delete: {len(failed)} entries")
            print(f"    Keys: {', '.join(failed)}")

    def _cmd_purge(self, inspector: CacheInspector, dry_run: bool):
        """Purge expired and corrupted entries."""
        print("Scanning for expired and corrupted entries...")
        result = inspector.purge_expired(dry_run=dry_run)

        mode = "DRY RUN" if dry_run else "ACTUAL PURGE"
        print(f"\n{'='*60}")
        print(f"PURGE RESULTS ({mode})")
        print(f"{'='*60}")

        print(
            f"Total entries scanned: {result['expired_count'] + result['valid_count']}"
        )
        print(f"Valid entries: {result['valid_count']}")
        print(f"Expired/Corrupted entries: {result['expired_count']}")
        print(f"Total size to reclaim: {self._format_size(result['total_size'])}")

        if dry_run:
            print(f"\nEntries that would be deleted:")
            for entry in result["entries"]:
                reason = []
                if entry["expired"]:
                    reason.append("expired")
                if entry["corrupted"]:
                    reason.append("corrupted")
                print(
                    f"  Key {entry['key']}: {self._format_size(entry['size'])} ({', '.join(reason)})"
                )
        else:
            print(f"\nEntries deleted: {result.get('deleted_count', 0)}")

    def _cmd_stats(self, inspector: CacheInspector):
        """Show cache statistics."""
        entries = inspector.list_entries(show_expired=True)

        if not entries:
            print("Cache is empty.")
            return

        # Calculate statistics
        total_size = sum(e["size"] for e in entries)
        expired_count = sum(1 for e in entries if e["is_expired"])
        corrupted_count = 0
        for entry in entries:
            try:
                inspector._read_cache_file(entry["key"])
            except ValueError:
                corrupted_count += 1

        # Find most/least accessed
        access_counts = [e["access_count"] for e in entries]
        if access_counts:
            max_access = max(access_counts)
            min_access = min(access_counts)
            avg_access = sum(access_counts) / len(access_counts)

        print(f"\n{'='*60}")
        print(f"CACHE STATISTICS")
        print(f"{'='*60}")
        print(f"Cache directory: {inspector.cache_dir}")
        print(f"Metadata file: {inspector.meta_path}")
        print(f"\nGeneral:")
        print(f"  Total entries: {len(entries)}")
        print(f"  Total size: {self._format_size(total_size)}")
        print(
            f"  Expired entries: {expired_count} ({expired_count/len(entries)*100:.1f}%)"
        )
        print(
            f"  Corrupted entries: {corrupted_count} ({corrupted_count/len(entries)*100:.1f}%)"
        )

        if access_counts:
            print(f"\nAccess patterns:")
            print(f"  Most accessed: {max_access} times")
            print(f"  Least accessed: {min_access} times")
            print(f"  Average accesses: {avg_access:.1f} times")

        print(f"\nSize distribution:")
        sizes = [e["size"] for e in entries]
        if sizes:
            print(f"  Largest entry: {self._format_size(max(sizes))}")
            print(f"  Smallest entry: {self._format_size(min(sizes))}")
            print(f"  Average size: {self._format_size(sum(sizes)/len(sizes))}")

    def _cmd_repair(self, inspector: CacheInspector):
        """Repair metadata inconsistencies."""
        print("Repairing metadata...")
        result = inspector.repair_metadata()

        print(f"\nRepair complete:")
        print(f"  Orphaned cache files (added to metadata): {result['orphaned_files']}")
        print(f"  Orphaned metadata entries (removed): {result['orphaned_metadata']}")
        print(f"  Total entries after repair: {result['total_entries']}")

    def _interactive_mode(self, inspector: CacheInspector):
        """Enter interactive mode."""
        print(f"Cache Inspector Interactive Mode")
        print(f"Cache directory: {inspector.cache_dir}")
        print(f"Type 'help' for commands, 'quit' to exit\n")

        while True:
            try:
                command = input("cache> ").strip().split()

                if not command:
                    continue

                cmd = command[0].lower()

                if cmd == "quit" or cmd == "exit":
                    break
                elif cmd == "help":
                    self._show_interactive_help()
                elif cmd == "list":
                    show_expired = "--show-expired" in command
                    entries = inspector.list_entries(show_expired=show_expired)

                    if not entries:
                        print("No cache entries found.")
                    else:
                        for entry in sorted(entries, key=lambda x: x["key"]):
                            size_str = self._format_size(entry["size"])
                            expires_str = (
                                entry["expires_at"].strftime("%Y-%m-%d")
                                if entry["expires_at"]
                                else "No TTL"
                            )
                            print(
                                f"{entry['key']:>10} | {size_str:>8} | {entry['access_count']:>6} | "
                                f"{entry['status'][:15]:<15} | {expires_str}"
                            )

                elif cmd == "inspect" and len(command) > 1:
                    for key_str in command[1:]:
                        try:
                            key = int(key_str)
                            result = inspector.inspect_entry(key)

                            if not result["exists"]:
                                print(f"Key {key}: Not found")
                            elif result["valid"]:
                                print(
                                    f"Key {key}: VALID ({result.get('data_type', 'Unknown')})"
                                )
                                print(
                                    f"  Preview: {result.get('preview', 'No preview')[:100]}"
                                )
                            else:
                                print(
                                    f"Key {key}: CORRUPT - {result.get('error', 'Unknown error')}"
                                )
                        except ValueError:
                            print(f"Invalid key: {key_str}")

                elif cmd == "delete" and len(command) > 1:
                    keys = []
                    for key_str in command[1:]:
                        try:
                            keys.append(int(key_str))
                        except ValueError:
                            print(f"Invalid key: {key_str}")

                    if keys:
                        confirm = input(f"Delete {len(keys)} entries? (y/N): ")
                        if confirm.lower() == "y":
                            for key in keys:
                                inspector.delete_entry(key)
                                print(f"Deleted key: {key}")

                elif cmd == "purge":
                    dry_run = "--dry-run" in command
                    print("Scanning for expired/corrupted entries...")
                    result = inspector.purge_expired(dry_run=dry_run)

                    mode = "Would delete" if dry_run else "Deleted"
                    print(
                        f"{mode} {result['expired_count']} entries "
                        f"({self._format_size(result['total_size'])})"
                    )

                    if dry_run and result["entries"]:
                        print("\nEntries to delete:")
                        for entry in result["entries"][:10]:  # Show first 10
                            reason = []
                            if entry["expired"]:
                                reason.append("expired")
                            if entry["corrupted"]:
                                reason.append("corrupted")
                            print(f"  {entry['key']} ({', '.join(reason)})")

                        if len(result["entries"]) > 10:
                            print(f"  ... and {len(result['entries']) - 10} more")

                elif cmd == "stats":
                    entries = inspector.list_entries(show_expired=True)
                    if entries:
                        total_size = sum(e["size"] for e in entries)
                        expired = sum(1 for e in entries if e["is_expired"])
                        print(f"Entries: {len(entries)}")
                        print(f"Total size: {self._format_size(total_size)}")
                        print(f"Expired: {expired} ({expired/len(entries)*100:.1f}%)")
                    else:
                        print("Cache is empty.")

                else:
                    print(f"Unknown command: {cmd}")
                    print("Type 'help' for available commands.")

            except (KeyboardInterrupt, EOFError):
                print("\nExiting...")
                break
            except Exception as e:
                print(f"Error: {e}")

    def _show_interactive_help(self):
        """Show help for interactive mode."""
        help_text = """
        Available commands:
          list [--show-expired]    - List cache entries
          inspect <key1> [key2...] - Inspect specific entries
          delete <key1> [key2...]  - Delete specific entries
          purge [--dry-run]        - Purge expired/corrupted entries
          stats                    - Show cache statistics
          repair                   - Repair metadata inconsistencies
          help                     - Show this help
          quit                     - Exit interactive mode
        """
        print(textwrap.dedent(help_text).strip())

    def _format_size(self, bytes: int) -> str:
        """Format bytes to human readable size."""
        for unit in ["B", "KB", "MB", "GB"]:
            if bytes < 1024.0:
                return f"{bytes:.1f}{unit}"
            bytes /= 1024.0
        return f"{bytes:.1f}TB"


def main():
    """Main entry point."""
    cli = CLI()
    cli.run()


if __name__ == "__main__":
    main()
