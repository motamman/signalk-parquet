import { Brand, Context, Path, Timestamp } from '@signalk/server-api';
import { Request } from 'express';

export type AggregateMethod = Brand<string, 'aggregatemethod'>;

type ValueList = {
  path: Path;
  method: AggregateMethod;
}[];

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export type Datarow = [Timestamp, ...any[]];

export interface DataResult {
  context: Context;
  range: {
    from: Timestamp;
    to: Timestamp;
  };
  values: ValueList;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  data: Datarow[];
}
// eslint-disable-next-line @typescript-eslint/no-unused-vars
const _demo: DataResult = {
  context:
    'vessels.urn:mrn:signalk:uuid:2ffee4a6-52f6-4d4e-8179-0fc9aaf22c87' as Context,
  range: {
    from: '2025-08-11T05:26:04.888Z' as Timestamp,
    to: '2025-08-11T05:41:04.888Z' as Timestamp,
  },
  values: [
    {
      path: 'navigation.speedOverGround' as Path,
      method: 'average' as AggregateMethod,
    },
  ],
  data: [
    ['2025-08-11T05:26:05.000Z' as Timestamp, null],
    ['2025-08-11T05:26:10.000Z' as Timestamp, 3.14],
  ],
};

export interface ValuesResponse extends DataResult {
  context: Context;
  range: {
    from: Timestamp;
    to: Timestamp;
  };
}

export type FromToContextRequest = Request<
  unknown,
  unknown,
  unknown,
  {
    from?: string;
    to?: string;
    start?: string;
    duration?: string;
    context: string;
    bbox?: string;
    refresh?: string;
    useUTC?: string;
  }
>;

export interface PathSpec {
  path: Path;
  queryResultName: string;
  aggregateMethod: AggregateMethod;
  aggregateFunction: string;
}

export interface HistoryAPIValue {
  timestamp: string;
  value: any;
}

export { Context } from '@signalk/server-api';
