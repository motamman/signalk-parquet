// Resolves a self-hosted S3-compatible endpoint (e.g. Garage, MinIO) into the
// pieces both the AWS SDK client and DuckDB's S3 extension need.
export interface ResolvedS3Endpoint {
  url: string; // original endpoint, validated (includes scheme)
  host: string; // host[:port], no scheme - what DuckDB's ENDPOINT expects
  useSSL: boolean;
  forcePathStyle: boolean;
}

export function resolveCustomS3Endpoint(
  endpoint: string,
  forcePathStyleConfig: boolean | undefined
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

  return {
    url: endpoint,
    host: parsed.host,
    useSSL: parsed.protocol === 'https:',
    forcePathStyle:
      forcePathStyleConfig !== undefined ? forcePathStyleConfig : true,
  };
}
