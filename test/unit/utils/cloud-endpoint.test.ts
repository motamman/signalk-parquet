/**
 * Unit tests for custom S3 endpoint resolution. The security-relevant behaviour
 * is the SSRF guard: a custom endpoint pointing at a private/loopback/link-local
 * host must be rejected unless the operator explicitly opts in.
 */
import { expect } from 'chai';
import { resolveCustomS3Endpoint } from '../../../src/utils/cloud-endpoint';

describe('resolveCustomS3Endpoint', () => {
  const privateHosts = [
    'http://169.254.169.254', // cloud metadata
    'http://localhost:9000',
    'http://127.0.0.1',
    'http://[::1]',
    'http://10.1.2.3',
    'http://192.168.1.10',
    'http://172.16.0.1',
    'http://[::ffff:127.0.0.1]', // IPv4-mapped IPv6 loopback
    'http://[::ffff:169.254.169.254]', // IPv4-mapped IPv6 metadata
    'http://[fe80::1]', // link-local (bottom of fe80::/10)
    'http://[febf::1]', // link-local (top of fe80::/10)
  ];

  it('rejects private/loopback/link-local hosts by default', () => {
    for (const url of privateHosts) {
      expect(() => resolveCustomS3Endpoint(url, undefined), url).to.throw();
    }
  });

  it('permits private hosts when allowPrivateEndpoint is true', () => {
    for (const url of privateHosts) {
      expect(
        () => resolveCustomS3Endpoint(url, undefined, true),
        url
      ).to.not.throw();
    }
  });

  it('accepts a public endpoint host', () => {
    const r = resolveCustomS3Endpoint('https://s3.example.com', undefined);
    expect(r.host).to.equal('s3.example.com');
    expect(r.useSSL).to.equal(true);
  });

  it('accepts a public IPv6 host', () => {
    expect(() =>
      resolveCustomS3Endpoint('https://[2606:4700::1]', undefined)
    ).to.not.throw();
  });

  it('still rejects an endpoint with a path or query', () => {
    expect(() =>
      resolveCustomS3Endpoint('https://s3.example.com/bucket', undefined)
    ).to.throw();
    expect(() =>
      resolveCustomS3Endpoint('https://s3.example.com?x=1', undefined)
    ).to.throw();
  });

  it('still rejects a non-http(s) scheme', () => {
    expect(() =>
      resolveCustomS3Endpoint('ftp://s3.example.com', undefined)
    ).to.throw();
  });
});
