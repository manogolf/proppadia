#!/usr/bin/env bash

# Write sensitive HTTP headers to a short-lived, owner-only file so curl's
# process arguments contain only the file path, never the credential value.
proppadia_curl_auth_headers_create() {
  if [[ $# -lt 1 ]]; then
    echo "curl_auth_headers: at least one header is required" >&2
    return 2
  fi
  umask 077
  PROPPADIA_CURL_AUTH_HEADERS_FILE="$(mktemp "${TMPDIR:-/tmp}/proppadia-curl-headers.XXXXXX")" || return 1
  chmod 600 "${PROPPADIA_CURL_AUTH_HEADERS_FILE}"
  if ! printf '%s\n' "$@" > "${PROPPADIA_CURL_AUTH_HEADERS_FILE}"; then
    rm -f "${PROPPADIA_CURL_AUTH_HEADERS_FILE}"
    PROPPADIA_CURL_AUTH_HEADERS_FILE=""
    return 1
  fi
}

proppadia_curl_auth_headers_cleanup() {
  if [[ -n "${PROPPADIA_CURL_AUTH_HEADERS_FILE:-}" ]]; then
    rm -f "${PROPPADIA_CURL_AUTH_HEADERS_FILE}"
    PROPPADIA_CURL_AUTH_HEADERS_FILE=""
  fi
}
