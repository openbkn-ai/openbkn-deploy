#!/usr/bin/env python3
# Copyright openbkn.ai
#
# Licensed under the OpenBKN License. See LICENSE-OPENBKN.txt in the project root.

"""Create an S3 bucket if it is absent, using only the Python standard library.

Usage: S3_ACCESS_KEY=... S3_SECRET_KEY=... s3_bucket.py <endpoint> <bucket>

Credentials come from the environment so they never appear in argv. The
request is a SigV4-signed `PUT /<bucket>`, so it works against any MinIO image,
including builds that ship without the `mc` client. Exit 0 when the bucket
exists afterwards (created now or already owned), 1 otherwise.
"""
import datetime
import hashlib
import hmac
import os
import sys
import urllib.error
import urllib.parse
import urllib.request

REGION = os.environ.get("S3_REGION", "us-east-1")
EMPTY_SHA256 = hashlib.sha256(b"").hexdigest()


def _sign(key, message):
    return hmac.new(key, message.encode(), hashlib.sha256).digest()


def signed_put_request(endpoint, bucket, access_key, secret_key, now=None):
    now = now or datetime.datetime.now(datetime.timezone.utc)
    amz_date = now.strftime("%Y%m%dT%H%M%SZ")
    day = now.strftime("%Y%m%d")
    parsed = urllib.parse.urlsplit(endpoint)
    host = parsed.netloc
    path = "/" + urllib.parse.quote(bucket, safe="")
    headers = {"host": host, "x-amz-content-sha256": EMPTY_SHA256, "x-amz-date": amz_date}
    signed_headers = ";".join(sorted(headers))
    canonical = "\n".join([
        "PUT", path, "",
        "".join(f"{name}:{headers[name]}\n" for name in sorted(headers)),
        signed_headers, EMPTY_SHA256,
    ])
    scope = f"{day}/{REGION}/s3/aws4_request"
    to_sign = "\n".join(["AWS4-HMAC-SHA256", amz_date, scope, hashlib.sha256(canonical.encode()).hexdigest()])
    key = _sign(_sign(_sign(_sign(("AWS4" + secret_key).encode(), day), REGION), "s3"), "aws4_request")
    signature = hmac.new(key, to_sign.encode(), hashlib.sha256).hexdigest()
    request = urllib.request.Request(f"{parsed.scheme}://{host}{path}", method="PUT", data=b"")
    request.add_header("x-amz-content-sha256", EMPTY_SHA256)
    request.add_header("x-amz-date", amz_date)
    request.add_header("Authorization", f"AWS4-HMAC-SHA256 Credential={access_key}/{scope}, "
                                        f"SignedHeaders={signed_headers}, Signature={signature}")
    return request


def ensure_bucket(endpoint, bucket, access_key, secret_key, timeout=15):
    request = signed_put_request(endpoint, bucket, access_key, secret_key)
    # The endpoint is a local port-forward: never route it through a shell proxy.
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    try:
        with opener.open(request, timeout=timeout):
            return "created"
    except urllib.error.HTTPError as error:
        body = error.read().decode(errors="replace")
        if error.code == 409 and "<Code>BucketAlreadyOwnedByYou</Code>" in body:
            return "exists"
        code = body.split("<Code>", 1)[1].split("</Code>", 1)[0] if "<Code>" in body else ""
        raise RuntimeError(f"HTTP {error.code} {code}".strip()) from None


def main(argv):
    if len(argv) != 3:
        print(__doc__.strip().splitlines()[2], file=sys.stderr)
        return 2
    access_key, secret_key = os.environ.get("S3_ACCESS_KEY", ""), os.environ.get("S3_SECRET_KEY", "")
    if not access_key or not secret_key:
        print("S3_ACCESS_KEY and S3_SECRET_KEY are required", file=sys.stderr)
        return 2
    try:
        print(ensure_bucket(argv[1], argv[2], access_key, secret_key))
    except (RuntimeError, urllib.error.URLError, OSError) as error:
        # Never echo the request: its Authorization header is derived from the secret.
        print(f"bucket request failed: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
