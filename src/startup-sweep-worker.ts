/**
 * Startup sweep worker.
 *
 * Runs the crash-recovery sweeps (see services/startup-sweeps.ts) in a
 * short-lived forked process so that walking a large store never holds the
 * Signal K server's main thread.
 *
 * Protocol (parent ⇄ this worker, over child_process IPC):
 *   parent → worker: one { dataDir } message.
 *   worker → parent: {type:'log'} lines (relayed to app.debug/error), then
 *                    exactly one {type:'result'} or {type:'error'}, then exit.
 * Any other message (the plugin's shutdown request, for one) is ignored;
 * the parent kills a worker that outlives its grace period.
 */
import {
  runStartupSweeps,
  SweepWorkerMessage,
} from './services/startup-sweeps';

function send(msg: SweepWorkerMessage): void {
  process.send?.(msg);
}

let started = false;
process.on('message', (msg: { dataDir?: unknown }) => {
  if (started || !msg || typeof msg !== 'object') return;
  if (typeof msg.dataDir !== 'string') return;
  started = true;
  runStartupSweeps(msg.dataDir, {
    debug: m => send({ type: 'log', level: 'debug', msg: m }),
    error: m => send({ type: 'log', level: 'error', msg: m }),
  })
    .then(result => {
      send({ type: 'result', result });
    })
    .catch(err => {
      send({ type: 'error', message: (err as Error).message });
      process.exitCode = 1;
    })
    .finally(() => {
      // Give the IPC message a tick to flush, then exit.
      setTimeout(() => process.exit(process.exitCode ?? 0), 50);
    });
});
