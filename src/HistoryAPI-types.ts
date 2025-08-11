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
  values: ValueList;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  data: Datarow[];
}

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
    from: string;
    to: string;
    context: string;
    bbox: string;
  }
>;

export interface PathSpec {
  path: Path;
  queryResultName: string;
  aggregateMethod: AggregateMethod;
  aggregateFunction: string;
}
