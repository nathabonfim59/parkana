import { DataSourcePlugin } from '@grafana/data';

import { ConfigEditor } from './components/ConfigEditor';
import { QueryEditor } from './components/QueryEditor';
import { DataSource } from './datasource';
import { ParkanaDataSourceOptions, ParkanaQuery } from './types';

export const plugin = new DataSourcePlugin<DataSource, ParkanaQuery, ParkanaDataSourceOptions>(DataSource)
  .setConfigEditor(ConfigEditor)
  .setQueryEditor(QueryEditor);
