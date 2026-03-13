from __future__ import annotations

import argparse
import json

from boxer.routers.common.request_audit import _run_request_audit_backup_job


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Backup request audit SQLite snapshot to configured S3",
    )
    parser.parse_args()

    result = _run_request_audit_backup_job()
    print(json.dumps(result, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
