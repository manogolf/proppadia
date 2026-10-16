import os
import subprocess
from pathlib import Path


def test_curl_auth_headers_are_private_and_removed(tmp_path):
    repo = Path(__file__).resolve().parents[2]
    helper = repo / "bin" / "curl_auth_headers.sh"
    marker = tmp_path / "header-path.txt"
    script = "\n".join(
        [
            f'source "{helper}"',
            'proppadia_curl_auth_headers_create "X-Ops-Token: ${TEST_AUTH_TOKEN}"',
            f'printf "%s\\n" "$(stat -f %Lp "$PROPPADIA_CURL_AUTH_HEADERS_FILE")" > "{marker}"',
            f'cat "$PROPPADIA_CURL_AUTH_HEADERS_FILE" >> "{marker}"',
            "trap proppadia_curl_auth_headers_cleanup EXIT",
        ]
    )
    env = {
        "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
        "TEST_AUTH_TOKEN": "fixture-only-token",
        "TMPDIR": str(tmp_path),
    }
    subprocess.run(["bash", "-c", script], env=env, check=True, capture_output=True, text=True)

    lines = marker.read_text().splitlines()
    assert lines == ["600", "X-Ops-Token: fixture-only-token"]
    assert not list(tmp_path.glob("proppadia-curl-headers.*"))


def test_remote_ops_client_keeps_token_out_of_curl_arguments(tmp_path):
    repo = Path(__file__).resolve().parents[2]
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    fake_curl = fake_bin / "curl"
    fake_curl.write_text(
        "#!/usr/bin/env bash\n"
        "set -euo pipefail\n"
        "header_file=''\n"
        "for ((i=1; i<=$#; i++)); do\n"
        "  arg=\"${!i}\"\n"
        "  [[ \"${arg}\" != *fixture-only-token* ]] || exit 20\n"
        "  if [[ \"${arg}\" == '-H' ]]; then\n"
        "    j=$((i+1)); header_file=\"${!j}\"\n"
        "  fi\n"
        "done\n"
        "[[ \"${header_file}\" == @* ]] || exit 21\n"
        "grep -qx 'X-Ops-Token: fixture-only-token' \"${header_file#@}\"\n"
        "printf '{\"status\":\"ok\"}'\n"
    )
    fake_curl.chmod(0o700)
    env = {
        "PATH": f"{fake_bin}:{os.environ.get('PATH', '/usr/bin:/bin')}",
        "PROPPADIA_BACKEND_URL": "https://backend.invalid",
        "OPS_API_TOKEN": "fixture-only-token",
        "TMPDIR": str(tmp_path),
    }
    result = subprocess.run(
        ["bash", str(repo / "bin/mlb_prod12_remote_status.sh"), "0"],
        env=env,
        check=True,
        capture_output=True,
        text=True,
    )
    assert '{"status":"ok"}' in result.stdout
    assert not list(tmp_path.glob("proppadia-curl-headers.*"))
