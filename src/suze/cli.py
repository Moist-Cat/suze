"""`suze` CLI entry point."""

import argparse
import json
import sys
from pathlib import Path

from suze.client import Client, default_socket
from suze.protocol import RemoteError


def _fmt_table(rows: list[list[str]], headers: list[str]) -> str:
    widths = [len(h) for h in headers]
    for row in rows:
        for i, cell in enumerate(row):
            widths[i] = max(widths[i], len(cell))
    line = "  ".join(h.ljust(widths[i]) for i, h in enumerate(headers))
    sep = "  ".join("-" * widths[i] for i in range(len(headers)))
    body = "\n".join(
        "  ".join(c.ljust(widths[i]) for i, c in enumerate(row)) for row in rows
    )
    return f"{line}\n{sep}\n{body}"


def _print_json(obj) -> None:
    print(json.dumps(obj, indent=2, default=str))


def cmd_list(args, client: Client) -> int:
    data = client.call("list")
    if args.json:
        _print_json(data)
        return 0
    rows = []
    for pid, s in sorted(data["pipelines"].items()):
        rows.append([pid, s["status"], f"{s['completed']}/{s['total']}",
                     str(s["failed"]), s["error"] or ""])
    if rows:
        print("Pipelines:")
        print(_fmt_table(rows, ["ID", "STATUS", "DONE", "FAIL", "ERROR"]))
    sched = []
    for pid, s in sorted(data["scheduler"].items()):
        sched.append([pid, s["trigger"], "on" if s["enabled"] else "off",
                      str(s["cost"]), s["last_status"] or "-"])
    if sched:
        print("\nScheduler:")
        print(_fmt_table(sched, ["ID", "TRIGGER", "ENABLED", "COST", "LAST"]))
    c = data["credits"]
    print(f"\nCredits: {c['available']}/{c['total']} available")
    return 0


def cmd_status(args, client: Client) -> int:
    if not args.pipeline_id:
        return cmd_list(args, client)
    data = client.call("status", pipeline_id=args.pipeline_id)
    if args.json:
        _print_json(data)
    else:
        for k, v in data.items():
            print(f"{k}: {v}")
    return 0


def cmd_view(args, client: Client) -> int:
    data = client.call("view", pipeline_id=args.pipeline_id, format=args.format)
    print(data["content"])
    return 0


def _simple(action: str):
    def _run(args, client: Client) -> int:
        result = client.call(action, pipeline_id=args.pipeline_id)
        if args.json:
            _print_json(result)
        else:
            print(f"{action}: {result}")
        return 0
    return _run


def cmd_report(args, client: Client) -> int:
    _print_json(client.call("report"))
    return 0


def cmd_shutdown(args, client: Client) -> int:
    _print_json(client.call("shutdown"))
    return 0


def cmd_daemon(args) -> int:
    from suze.daemon import main as daemon_main
    return daemon_main(args.config)


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="suze")
    p.add_argument("--socket", type=Path, default=None)
    p.add_argument("--json", action="store_true")
    sub = p.add_subparsers(dest="cmd", required=True)

    d = sub.add_parser("daemon", help="run the daemon (foreground; use systemd in prod)")
    d.add_argument("--config", action="append", required=True)
    d.set_defaults(func=cmd_daemon, needs_client=False)

    sub.add_parser("list", aliases=["ps"]).set_defaults(func=cmd_list)

    s = sub.add_parser("status")
    s.add_argument("pipeline_id", nargs="?")
    s.set_defaults(func=cmd_status)

    v = sub.add_parser("view")
    v.add_argument("pipeline_id")
    v.add_argument("--format", choices=["mermaid", "graphviz"], default="mermaid")
    v.set_defaults(func=cmd_view)

    for action in ("pause", "resume", "cancel", "trigger", "enable", "disable"):
        sp = sub.add_parser(action)
        sp.add_argument("pipeline_id")
        sp.set_defaults(func=_simple(action))

    sub.add_parser("report").set_defaults(func=cmd_report)
    sub.add_parser("shutdown").set_defaults(func=cmd_shutdown)
    return p


def main(argv=None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if getattr(args, "needs_client", True) is False:
        return args.func(args)

    client = Client(socket_path=args.socket or default_socket())
    try:
        return args.func(args, client)
    except RemoteError as e:
        print(f"error: {e.message}", file=sys.stderr)
        return 1
    except ConnectionError as e:
        print(f"error: {e}", file=sys.stderr)
        return 3


if __name__ == "__main__":
    sys.exit(main())
