import { Router, Request, Response } from 'express';
import {
  AggregateMethod,
  DataResult,
  FromToContextRequest,
  PathSpec,
} from './HistoryAPI-types';
import { ZonedDateTime } from '@js-joda/core';
import { Context, Path } from '@signalk/server-api';
import { ParamsDictionary } from 'express-serve-static-core';
import { ParsedQs } from 'qs';
import { DuckDBInstance } from '@duckdb/node-api';
import { toContextFilePath } from '.';
import path from 'path';

export function registerHistoryApiRoute(
  router: Pick<Router, 'get'>,
  selfId: string,
  dataDir: string,
  debug: (k: string) => void
) {
  const historyApi = new HistoryAPI(selfId, dataDir);
  router.get('/signalk/v1/history/values', (req: Request, res: Response) => {
    const { from, to, context } = getRequestParams(
      req as FromToContextRequest,
      selfId
    );
    historyApi.getValues(context, from, to, debug, req, res);
  });
  router.get('/signalk/v1/history/contexts', (req: Request, res: Response) => {
    //TODO implement retrieval of contexts for the given period
    res.json([`vessels.${selfId}`] as Context[]);
  });
  router.get('/signalk/v1/history/paths', (req: Request, res: Response) => {
    //TODO implement retrieval of paths for the given period
    // const { from, to } = getRequestParams(req as FromToContextRequest, selfId);
    // getPaths(influx, from, to, res);
    res.json(['navigation.speedOverGround']);
  });
}

const getRequestParams = ({ query }: FromToContextRequest, selfId: string) => {
  try {
    const from = ZonedDateTime.parse(query['from']);
    const to = ZonedDateTime.parse(query['to']);
    const context: Context = getContext(query.context, selfId);
    const bbox = query['bbox'];
    return { from, to, context, bbox };
  } catch (e: unknown) {
    throw new Error(
      `Error extracting from/to query parameters from ${JSON.stringify(query)}`
    );
  }
};

function getContext(contextFromQuery: string, selfId: string): Context {
  if (
    !contextFromQuery ||
    contextFromQuery === 'vessels.self' ||
    contextFromQuery === 'self'
  ) {
    return `vessels.${selfId}` as Context;
  }
  return contextFromQuery.replace(/ /gi, '') as Context;
}

class HistoryAPI {
  readonly selfContextPath: string;
  constructor(
    private selfId: string,
    private dataDir: string
  ) {
    this.selfContextPath = toContextFilePath(`vessels.${selfId}` as Context);
  }
  async getValues(
    context: Context,
    from: ZonedDateTime,
    to: ZonedDateTime,
    debug: (k: string) => void,
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    req: Request<ParamsDictionary, any, any, ParsedQs, Record<string, any>>,
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    res: Response<any, Record<string, any>>
  ) {
    const timeResolutionMillis =
      (req.query.resolution
        ? Number.parseFloat(req.query.resolution as string)
        : (to.toEpochSecond() - from.toEpochSecond()) / 500) * 1000;
    const pathExpressions = ((req.query.paths as string) || '')
      .replace(/[^0-9a-z.,:]/gi, '')
      .split(',');
    const pathSpecs: PathSpec[] = pathExpressions.map(splitPathExpression);

    const positionPathSpecs = pathSpecs
      .filter(({ path }) => path === 'navigation.position')
      .slice(0, 1);
    // const positionResult = positionPathSpecs.length
    //   ? getPositions(context, from, to, timeResolutionMillis, debug)
    //   : Promise.resolve({
    //       values: [],
    //       data: [],
    //     });

    const nonPositionPathSpecs = pathSpecs.filter(
      ({ path }) => path !== 'navigation.position'
    );
    const nonPositionResult = nonPositionPathSpecs.length
      ? await this.getNumericValues(
          context,
          from,
          to,
          timeResolutionMillis,
          nonPositionPathSpecs,
          debug
        )
      : Promise.resolve({
          values: [],
          data: [],
        });

    //TODO consolidate position & numeric results
    res.json(nonPositionResult);
  }

  async getNumericValues(
    context: Context,
    from: ZonedDateTime,
    to: ZonedDateTime,
    timeResolutionMillis: number,
    pathSpecs: PathSpec[],
    debug: (k: string) => void
  ): Promise<DataResult> {
    const filePaths = pathSpecs.map(async pathSpec => {
      const filePath = path.join(
        this.dataDir,
        this.selfContextPath,
        pathSpec.path.replace(/\./g, '/'),
        '*.parquet'
      );
      const query = `
      SELECT
        *
      FROM '${filePath}'
      WHERE
        strptime(signalk_timestamp, '%Y-%m-%dT%H:%M:%-S.%gZ') >= ${from.toString()}::TIMESTAMP
        AND 
        strptime(signalk_timestamp, '%Y-%m-%dT%H:%M:%-S.%gZ') < ${to.toString()}::TIMESTAMP      
      `;
      debug(`Executing query: ${query}`);
      const duckDB = await DuckDBInstance.create();
      const connection = await duckDB.connect();
      const result = await connection.run(query);
      const rows = await result.getRows();
      console.log(rows);
    });
    return Promise.resolve({ values: [], data: [] } as DataResult);
  }
}

function splitPathExpression(pathExpression: string): PathSpec {
  const parts = pathExpression.split(':');
  let aggregateMethod = (parts[1] || 'average') as AggregateMethod;
  if (parts[0] === 'navigation.position') {
    aggregateMethod = 'first' as AggregateMethod;
  }
  return {
    path: parts[0] as Path,
    queryResultName: parts[0].replace(/\./g, '_'),
    aggregateMethod,
    aggregateFunction:
      (functionForAggregate[aggregateMethod] as string) || 'mean()',
  };
}

const functionForAggregate: { [key: string]: string } = {
  average: 'avg',
  min: 'min',
  max: 'max',
  first: 'first',
} as const;
