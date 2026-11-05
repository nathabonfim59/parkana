import type { CoreApp, DataSourceInstanceSettings } from '@grafana/data';
import { DataSourceWithBackend } from '@grafana/runtime';

import { DEFAULT_QUERY, type ParkanaDataSourceOptions, type ParkanaQuery } from './types';

/**
 * Parkana SQL datasource. Queries are executed by the plugin backend: the
 * frontend routes them through Grafana's backend datasource machinery and
 * never talks to the Parkana HTTP API itself.
 */
export class DataSource extends DataSourceWithBackend<ParkanaQuery, ParkanaDataSourceOptions> {
  constructor(instanceSettings: DataSourceInstanceSettings<ParkanaDataSourceOptions>) {
    super(instanceSettings);
  }

  getDefaultQuery(_: CoreApp): Partial<ParkanaQuery> {
    return DEFAULT_QUERY;
  }

  // Queries without SQL text would only produce a backend error, so skip them.
  filterQuery(query: ParkanaQuery): boolean {
    return Boolean(query.rawSQL?.trim());
  }
}
