import * as path from 'path';
import { Context } from '@signalk/server-api';

export function toContextFilePath(context: Context): string {
  return context.replace(/\./g, '/').replace(/:/g, '_');
}

export function toParquetFilePath(
  dataDir: string,
  selfContextPath: string,
  quotedPath: string
): string {
  return path.join(
    dataDir,
    selfContextPath,
    quotedPath.replace(/\./g, '/'),
    '*.parquet'
  );
}
