import type { DataSourceJsonData } from '@grafana/data';
import type { DataQuery } from '@grafana/schema';

/**
 * Shape of the result frame the backend should produce.
 *
 * `table` keeps every column of the SQL result. `time_series` expects the
 * query to yield a `time` column of type timestamp plus numeric columns and
 * returns one series per numeric column.
 */
export type QueryFormat = 'table' | 'time_series';

/** Value used by the editor when a query does not carry an explicit format. */
export const DEFAULT_FORMAT: QueryFormat = 'table';

/**
 * A Parkana SQL query. The SQL text is forwarded to the plugin backend
 * unchanged; `$__`-macros inside it are expanded there, never in the frontend.
 */
export interface ParkanaQuery extends DataQuery {
  /** Raw SQL sent to the Parkana backend. `$__`-macros are expanded server-side. */
  rawSQL?: string;
  /** Result shape requested from the backend. */
  format?: QueryFormat;
  /** Optional cap on returned rows. Unset means the server default applies. */
  limit?: number;
}

/** Default query seeded into a fresh editor row. */
export const DEFAULT_QUERY: Partial<ParkanaQuery> = {
  rawSQL: 'SELECT _ts, line\nFROM logs\nWHERE _ts >= $__timeFrom AND _ts < $__timeTo',
  format: DEFAULT_FORMAT,
};

/**
 * These are options configured for each Parkana datasource instance. Both
 * fields end up in the datasource settings JSON that the plugin backend
 * reads; the frontend itself never contacts the URL.
 */
export interface ParkanaDataSourceOptions extends DataSourceJsonData {
  /** Base URL of the Parkana HTTP API, e.g. http://parkana:3100. */
  url: string;
  /** Optional tenant ID for Parkana requests (X-Scope-OrgID). Empty uses the server default. */
  tenant?: string;
}

/**
 * Turns the text of the row-limit input into a query limit. Blank input and
 * anything that is not a positive safe whole integer yield `undefined`,
 * meaning "let the server apply its default". Fractional values are rejected
 * rather than silently rounded.
 */
export function parseLimitInput(value: string | undefined): number | undefined {
  if (value === undefined) {
    return undefined;
  }
  const trimmed = value.trim();
  if (trimmed === '') {
    return undefined;
  }
  const parsed = Number(trimmed);
  if (!Number.isSafeInteger(parsed) || parsed < 1) {
    return undefined;
  }
  return parsed;
}

/** Renders a query limit for display: `undefined` becomes the empty string. */
export function formatLimitInput(limit: number | undefined): string {
  return limit === undefined ? '' : String(limit);
}
