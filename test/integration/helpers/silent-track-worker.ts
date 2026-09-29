/**
 * A Track API worker that reports ready and then never answers a request:
 * the hung worker TrackWorkerClient's request timeout exists for.
 */
import type {
  TrackWorkerInbound,
  TrackWorkerOutbound,
} from '../../../src/utils/track-worker-protocol';

process.on('message', (msg: TrackWorkerInbound) => {
  if (msg.type === 'init') {
    process.send!({ type: 'ready' } satisfies TrackWorkerOutbound);
  } else if (msg.type === 'shutdown') {
    process.exit(0);
  }
});
