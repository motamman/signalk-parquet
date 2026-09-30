/**
 * What crosses to the Track API worker. The server passes Temporal objects,
 * whose fields live behind the prototype and do not survive a structured
 * clone, so instants and durations are converted before they are sent, to
 * forms the provider reads as the same values.
 */
import { expect } from 'chai';
import * as v8 from 'v8';
import { Temporal } from '@js-temporal/polyfill';
import {
  durationToMillis,
  instantToMillis,
  resolveWindow,
} from '../../src/track-provider';
import { toWireRequest } from '../../src/utils/track-worker-client';

/** A round trip through the serializer `serialization: 'advanced'` uses. */
const clone = <T>(value: T): T => v8.deserialize(v8.serialize(value)) as T;

describe('toWireRequest', () => {
  it('is why the conversion exists: a Temporal.Instant does not survive the clone', () => {
    const cloned = clone({ at: Temporal.Instant.from('2024-06-01T00:00:00Z') });
    expect(() => instantToMillis(cloned.at)).to.throw(/Invalid timestamp/);
  });

  it('sends instants as the same epoch milliseconds', () => {
    const query = {
      from: Temporal.Instant.from('2024-06-01T00:00:00.123Z'),
      to: '2024-06-02T00:00:00Z',
    };
    const wire = clone(toWireRequest(query));
    expect(wire.from).to.equal(Date.parse('2024-06-01T00:00:00.123Z'));
    expect(wire.to).to.equal(Date.parse('2024-06-02T00:00:00Z'));
    expect(resolveWindow(wire, 0)).to.deep.equal(resolveWindow(query, 0));
  });

  it('sends durations as exactly the same milliseconds, whatever form they came in', () => {
    for (const duration of [
      Temporal.Duration.from('PT2M'),
      Temporal.Duration.from({ seconds: 1, milliseconds: 1 }),
      'PT1.5S',
      90.001,
    ]) {
      const wire = clone(toWireRequest({ duration, resolution: duration }));
      expect(durationToMillis(wire.duration!)).to.equal(durationToMillis(duration));
      expect(durationToMillis(wire.resolution!)).to.equal(durationToMillis(duration));
    }
  });

  it('passes everything else through unchanged', () => {
    const query = {
      bbox: [1, 2, 3, 4] as [number, number, number, number],
      maxPoints: 10,
      simplify: true,
      properties: ['navigation.speedOverGround'],
    };
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    expect(clone(toWireRequest(query as any))).to.deep.equal(query);
  });

  it('refuses an invalid instant on the plugin side', () => {
    expect(() => toWireRequest({ from: 'not a time' })).to.throw(/Invalid timestamp/);
  });
});
