/**
 * Startup sweep worker.
 *
 * Runs the crash-recovery sweeps (see services/startup-sweeps.ts) in a
 * short-lived forked process so that walking a large store never holds the
 * Signal K server's main thread.
 *
 * Protocol (parent ⇄ this worker, over child_process IPC):
 *   parent → worker: one { dataDir } message, and possibly the plugin's
 *                    { type: 'shutdown' } request.
 *   worker → parent: {type:'log'} lines (relayed to app.debug/error), then
 *                    exactly one {type:'result'} or {type:'error'}, then exit.
 *
 * On shutdown it reports an incomplete result and exits at once, rather than
 * making plugin.stop() wait out its grace period and kill it. Stopping a sweep
 * part way is what that kill did anyway, and what a crash does: the sweeps
 * are crash recovery, and the next start runs them again.
 */
import {
  emptySweepResult,
  runStartupSweeps,
  SweepWorkerMessage,
} from './services/startup-sweeps';

function send(msg: SweepWorkerMessage): void {
  process.send?.(msg);
}

/** Exit once the IPC message has had a tick to flush. */
function exitSoon(): void {
  setTimeout(() => process.exit(process.exitCode ?? 0), 50);
}

let started = false;
let finished = false;
process.on('message', (msg: { dataDir?: unknown; type?: unknown }) => {
  if (!msg || typeof msg !== 'object') return;
  if (msg.type === 'shutdown') {
    if (finished) return;
    finished = true;
    send({ type: 'result', result: emptySweepResult() });
    exitSoon();
    return;
  }
  if (started || typeof msg.dataDir !== 'string') return;
  started = true;
  runStartupSweeps(msg.dataDir, {
    debug: m => send({ type: 'log', level: 'debug', msg: m }),
    error: m => send({ type: 'log', level: 'error', msg: m }),
  })
    .then(result => {
      if (!finished) send({ type: 'result', result });
    })
    .catch(err => {
      if (!finished) send({ type: 'error', message: (err as Error).message });
      process.exitCode = 1;
    })
    .finally(() => {
      finished = true;
      exitSoon();
    });
});
