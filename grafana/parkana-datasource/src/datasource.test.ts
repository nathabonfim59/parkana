import type { CoreApp, DataSourceInstanceSettings } from '@grafana/data';

import { DataSource } from './datasource';
import type { ParkanaDataSourceOptions, ParkanaQuery } from './types';

// Minimal stand-in for the runtime base class so the contract tests below
// exercise only the additions this datasource makes (getDefaultQuery and
// filterQuery) without initializing Grafana's runtime services.
jest.mock('@grafana/runtime', () => ({
  DataSourceWithBackend: class {
    instanceSettings: unknown;
    constructor(instanceSettings: unknown) {
      this.instanceSettings = instanceSettings;
    }
  },
}));

const instanceSettings = {
  uid: 'parkana-sql',
  url: '',
  jsonData: {
    url: 'http://parkana:3100',
    tenant: 'default',
  },
} as DataSourceInstanceSettings<ParkanaDataSourceOptions>;

describe('DataSource', () => {
  const datasource = new DataSource(instanceSettings);

  describe('getDefaultQuery', () => {
    it('seeds a table query using the time range macros', () => {
      const defaults = datasource.getDefaultQuery('explore' as CoreApp);
      expect(defaults.format).toBe('table');
      expect(defaults.rawSQL).toContain('$__timeFrom');
      expect(defaults.rawSQL).toContain('$__timeTo');
    });
  });

  describe('filterQuery', () => {
    it('keeps queries that carry SQL', () => {
      const query = { refId: 'A', rawSQL: 'SELECT 1', format: 'table' } as ParkanaQuery;
      expect(datasource.filterQuery(query)).toBe(true);
    });

    it('keeps queries whose SQL has surrounding whitespace', () => {
      const query = { refId: 'A', rawSQL: '  SELECT 1  ', format: 'table' } as ParkanaQuery;
      expect(datasource.filterQuery(query)).toBe(true);
    });

    it('drops empty and whitespace-only queries', () => {
      const empty = { refId: 'A', format: 'table' } as ParkanaQuery;
      const blank = { refId: 'A', rawSQL: '   ', format: 'table' } as ParkanaQuery;
      expect(datasource.filterQuery(empty)).toBe(false);
      expect(datasource.filterQuery(blank)).toBe(false);
    });
  });
});
