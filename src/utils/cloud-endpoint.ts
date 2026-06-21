// Resolves a self-hosted S3-compatible endpoint (e.g. Garage, MinIO) into the
// pieces both the AWS SDK client and DuckDB's S3 extension need.
export interface ResolvedS3Endpoint {
  url: string; // original endpoint, validated (includes scheme)
  host: string; // host[:port], no scheme - what DuckDB's ENDPOINT expects
  useSSL: boolean;
  forcePathStyle: boolean;
}

/**
 * True when `host` is a loopback, link-local, or RFC1918 private address (or a
 * localhost name). A custom S3 endpoint pointing at such a host turns the cloud
 * uploader (and DuckDB's httpfs) into an SSRF primitive against link-local cloud
 * metadata (169.254.169.254) or internal services, so these are rejected unless
 * the operator explicitly opts in.
 *
 * Input:  "169.254.169.254" -> true    Input: "s3.example.com" -> false
 * Input:  "127.0.0.1"        -> true    Input: "[::1]" / "::1"  -> true
 */
function isPrivateIpv4(a: number, b: number): boolean {
  if (a === 127 || a === 10 || a === 0) return true; // loopback, RFC1918 /8, unspecified
  if (a === 169 && b === 254) return true; // link-local incl. cloud metadata
  if (a === 172 && b >= 16 && b <= 31) return true; // RFC1918 /12
  if (a === 192 && b === 168) return true; // RFC1918 /16
  return false;
}

function isPrivateOrLoopbackHost(host: string): boolean {
  let h = host.toLowerCase().replace(/^\[/, '').replace(/\]$/, '');

  if (h === 'localhost' || h.endsWith('.localhost')) {
    return true;
  }

  // Canonicalize an IPv4-mapped IPv6 literal down to its embedded IPv4 so the
  // rules below catch it. Node's URL parser normalizes ::ffff:127.0.0.1 to the
  // hex form ::ffff:7f00:1, so handle both the dotted and hex encodings.
  const mappedDotted = h.match(/^::ffff:(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})$/);
  if (mappedDotted) {
    h = mappedDotted[1];
  } else {
    const mappedHex = h.match(/^::ffff:([0-9a-f]{1,4}):([0-9a-f]{1,4})$/);
    if (mappedHex) {
      const hi = parseInt(mappedHex[1], 16);
      const lo = parseInt(mappedHex[2], 16);
      h = `${hi >> 8}.${hi & 0xff}.${lo >> 8}.${lo & 0xff}`;
    }
  }

  // IPv4 (including a de-mapped ::ffff: address)
  const ipv4 = h.match(/^(\d{1,3})\.(\d{1,3})\.(\d{1,3})\.(\d{1,3})$/);
  if (ipv4) {
    const [a, b] = ipv4.slice(1).map(n => parseInt(n, 10));
    return isPrivateIpv4(a, b);
  }

  // IPv6
  if (h === '::1' || h === '::') return true; // loopback, unspecified
  // Link-local fe80::/10 spans the first hextet fe80–febf (not just fe80).
  if (/^fe[89ab][0-9a-f]:/.test(h)) return true;
  if (/^f[cd][0-9a-f]{2}:/.test(h)) return true; // unique-local fc00::/7
  return false;
}

export function resolveCustomS3Endpoint(
  endpoint: string,
  forcePathStyleConfig: boolean | undefined,
  allowPrivateEndpoint: boolean = false
): ResolvedS3Endpoint {
  let parsed: URL;
  try {
    parsed = new URL(endpoint);
  } catch {
    throw new Error(
      `cloudUpload.endpoint must be a full URL including scheme (e.g. "https://${endpoint}"), got: "${endpoint}"`
    );
  }

  if (parsed.protocol !== 'http:' && parsed.protocol !== 'https:') {
    throw new Error(
      `cloudUpload.endpoint must use http:// or https://, got: "${endpoint}"`
    );
  }

  if (!parsed.host) {
    throw new Error(`cloudUpload.endpoint is missing a host: "${endpoint}"`);
  }

  if (
    (parsed.pathname && parsed.pathname !== '/') ||
    parsed.search ||
    parsed.hash
  ) {
    throw new Error(
      `cloudUpload.endpoint must be an origin only (scheme + host[:port]), with no path, query, or fragment: "${endpoint}"`
    );
  }

  if (!allowPrivateEndpoint && isPrivateOrLoopbackHost(parsed.hostname)) {
    throw new Error(
      `cloudUpload.endpoint host "${parsed.hostname}" is a private, loopback, or link-local address; ` +
        `set cloudUpload.allowPrivateEndpoint to true to permit a self-hosted endpoint on your LAN: "${endpoint}"`
    );
  }

  return {
    url: endpoint,
    host: parsed.host,
    useSSL: parsed.protocol === 'https:',
    forcePathStyle:
      forcePathStyleConfig !== undefined ? forcePathStyleConfig : true,
  };
}
