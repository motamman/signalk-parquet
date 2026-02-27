/**
 * Hive Path Builder
 *
 * Constructs Hive-style partitioned paths for Parquet storage.
 * Target structure: tier=raw/context={ctx}/path={path}/year={year}/day={day}/
 */

import * as path from 'path';

export type AggregationTier = 'raw' | '5s' | '60s' | '1h';

export interface HivePath {
  basePath: string;
  tier: AggregationTier;
  context: string;
  signalkPath: string;
  year: number;
  dayOfYear: number;
  fullPath: string;
}

export interface PathParseResult {
  isHive: boolean;
  isFlat: boolean;
  tier?: AggregationTier;
  context?: string;
  signalkPath?: string;
  year?: number;
  dayOfYear?: number;
}

export class HivePathBuilder {
  /**
   * Build a Hive-style partitioned path
   */
  buildPath(
    basePath: string,
    tier: AggregationTier,
    context: string,
    signalkPath: string,
    timestamp: Date
  ): string {
    const year = timestamp.getUTCFullYear();
    const dayOfYear = this.getDayOfYear(timestamp);

    const sanitizedContext = this.sanitizeContext(context);
    const sanitizedPath = this.sanitizePath(signalkPath);

    return path.join(
      basePath,
      `tier=${tier}`,
      `context=${sanitizedContext}`,
      `path=${sanitizedPath}`,
      `year=${year}`,
      `day=${String(dayOfYear).padStart(3, '0')}`
    );
  }

  /**
   * Build a complete file path including filename
   */
  buildFilePath(
    basePath: string,
    tier: AggregationTier,
    context: string,
    signalkPath: string,
    timestamp: Date,
    filenamePrefix: string = 'data'
  ): string {
    const dirPath = this.buildPath(basePath, tier, context, signalkPath, timestamp);
    const timestampStr = timestamp
      .toISOString()
      .replace(/[:.]/g, '')
      .slice(0, 15);

    return path.join(dirPath, `${filenamePrefix}_${timestampStr}.parquet`);
  }

  /**
   * Parse a path to determine if it's Hive-style or flat
   */
  detectPathStyle(filePath: string): PathParseResult {
    // Check for Hive-style partition markers
    const hivePattern = /tier=([^/]+)\/context=([^/]+)\/path=([^/]+)\/year=(\d+)\/day=(\d+)/;
    const hiveMatch = filePath.match(hivePattern);

    if (hiveMatch) {
      return {
        isHive: true,
        isFlat: false,
        tier: hiveMatch[1] as AggregationTier,
        context: this.unsanitizeContext(hiveMatch[2]),
        signalkPath: this.unsanitizePath(hiveMatch[3]),
        year: parseInt(hiveMatch[4], 10),
        dayOfYear: parseInt(hiveMatch[5], 10),
      };
    }

    // Check for flat-style (legacy) path
    // Pattern: vessels/{context}/{path/parts}/filename.parquet
    const flatPattern = /vessels\/([^/]+)\/(.+?)\/[^/]+\.parquet$/;
    const flatMatch = filePath.match(flatPattern);

    if (flatMatch) {
      return {
        isHive: false,
        isFlat: true,
        context: `vessels.${flatMatch[1].replace(/_/g, ':')}`,
        signalkPath: flatMatch[2].replace(/\//g, '.'),
      };
    }

    return {
      isHive: false,
      isFlat: false,
    };
  }

  /**
   * Convert a flat-style path to Hive-style
   */
  flatToHive(
    flatPath: string,
    basePath: string,
    tier: AggregationTier = 'raw'
  ): string | null {
    const parsed = this.detectPathStyle(flatPath);

    if (!parsed.isFlat || !parsed.context || !parsed.signalkPath) {
      return null;
    }

    // Extract timestamp from filename
    const filenameMatch = path.basename(flatPath).match(
      /(\d{4})-?(\d{2})-?(\d{2})T?(\d{2})(\d{2})(\d{2})/
    );

    let timestamp: Date;
    if (filenameMatch) {
      timestamp = new Date(
        `${filenameMatch[1]}-${filenameMatch[2]}-${filenameMatch[3]}T${filenameMatch[4]}:${filenameMatch[5]}:${filenameMatch[6]}Z`
      );
    } else {
      // Use current time as fallback
      timestamp = new Date();
    }

    return this.buildFilePath(
      basePath,
      tier,
      parsed.context,
      parsed.signalkPath,
      timestamp,
      path.basename(flatPath, '.parquet')
    );
  }

  /**
   * Sanitize a context string for use in a path
   * vessels.urn:mrn:signalk:uuid:xxx -> vessels__urn-mrn-signalk-uuid-xxx
   */
  sanitizeContext(context: string): string {
    return context
      .replace(/\./g, '__')
      .replace(/:/g, '-');
  }

  /**
   * Unsanitize a context string from a path
   */
  unsanitizeContext(sanitized: string): string {
    return sanitized
      .replace(/__/g, '.')
      .replace(/-/g, ':');
  }

  /**
   * Sanitize a SignalK path for use in a file path
   * navigation.speedOverGround -> navigation__speedOverGround
   */
  sanitizePath(signalkPath: string): string {
    return signalkPath.replace(/\./g, '__');
  }

  /**
   * Unsanitize a path from a file path
   */
  unsanitizePath(sanitized: string): string {
    return sanitized.replace(/__/g, '.');
  }

  /**
   * Get the day of year (1-366)
   */
  getDayOfYear(date: Date): number {
    const start = new Date(Date.UTC(date.getUTCFullYear(), 0, 0));
    const diff = date.getTime() - start.getTime();
    const oneDay = 1000 * 60 * 60 * 24;
    return Math.floor(diff / oneDay);
  }

  /**
   * Create a date from year and day of year
   */
  dateFromDayOfYear(year: number, dayOfYear: number): Date {
    const date = new Date(Date.UTC(year, 0));
    date.setUTCDate(dayOfYear);
    return date;
  }

  /**
   * Get the glob pattern for finding files in a time range
   */
  getGlobPattern(
    basePath: string,
    tier: AggregationTier,
    context?: string,
    signalkPath?: string,
    year?: number,
    dayOfYear?: number
  ): string {
    const parts = [basePath, `tier=${tier}`];

    if (context) {
      parts.push(`context=${this.sanitizeContext(context)}`);
    } else {
      parts.push('context=*');
    }

    if (signalkPath) {
      parts.push(`path=${this.sanitizePath(signalkPath)}`);
    } else {
      parts.push('path=*');
    }

    if (year !== undefined) {
      parts.push(`year=${year}`);
    } else {
      parts.push('year=*');
    }

    if (dayOfYear !== undefined) {
      parts.push(`day=${String(dayOfYear).padStart(3, '0')}`);
    } else {
      parts.push('day=*');
    }

    parts.push('*.parquet');

    return path.join(...parts);
  }

  /**
   * Get all day directories in a time range
   */
  getDaysInRange(from: Date, to: Date): Array<{ year: number; dayOfYear: number }> {
    const days: Array<{ year: number; dayOfYear: number }> = [];
    const current = new Date(from);

    while (current <= to) {
      days.push({
        year: current.getUTCFullYear(),
        dayOfYear: this.getDayOfYear(current),
      });
      current.setUTCDate(current.getUTCDate() + 1);
    }

    return days;
  }

  /**
   * Build DuckDB-compatible glob pattern for Hive partitions
   */
  buildDuckDBGlob(
    basePath: string,
    tier: AggregationTier,
    context?: string,
    signalkPath?: string,
    fromDate?: Date,
    toDate?: Date
  ): string {
    // For DuckDB, we need to handle date ranges specially
    if (fromDate && toDate) {
      // If same year and day range is small, use explicit days
      const days = this.getDaysInRange(fromDate, toDate);
      if (days.length <= 7) {
        // Build explicit patterns for each day
        return days.map(d =>
          this.getGlobPattern(basePath, tier, context, signalkPath, d.year, d.dayOfYear)
        ).join(',');
      }
    }

    // Otherwise, use wildcards
    return this.getGlobPattern(basePath, tier, context, signalkPath);
  }
}
