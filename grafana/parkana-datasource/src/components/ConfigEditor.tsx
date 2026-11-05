import React, { ChangeEvent } from 'react';
import { InlineField, Input, Stack } from '@grafana/ui';
import type { DataSourcePluginOptionsEditorProps } from '@grafana/data';

import type { ParkanaDataSourceOptions } from '../types';

type Props = DataSourcePluginOptionsEditorProps<ParkanaDataSourceOptions>;

export function ConfigEditor({ onOptionsChange, options }: Props) {
  const { jsonData } = options;

  const onURLChange = (event: ChangeEvent<HTMLInputElement>) => {
    onOptionsChange({
      ...options,
      jsonData: {
        ...jsonData,
        url: event.currentTarget.value,
      },
    });
  };

  const onTenantChange = (event: ChangeEvent<HTMLInputElement>) => {
    onOptionsChange({
      ...options,
      jsonData: {
        ...jsonData,
        tenant: event.currentTarget.value,
      },
    });
  };

  return (
    <Stack gap={1} direction="column">
      <InlineField
        label="URL"
        labelWidth={14}
        required
        interactive
        tooltip="Base URL of the Parkana HTTP API. All SQL requests are made by the plugin backend."
      >
        <Input
          id="config-editor-url"
          onChange={onURLChange}
          value={jsonData.url ?? ''}
          placeholder="http://parkana:3100"
          width={40}
        />
      </InlineField>
      <InlineField
        label="Tenant"
        labelWidth={14}
        interactive
        tooltip="Optional tenant ID for Parkana requests (X-Scope-OrgID). Empty uses the server default tenant."
      >
        <Input
          id="config-editor-tenant"
          onChange={onTenantChange}
          value={jsonData.tenant ?? ''}
          placeholder="default"
          width={40}
        />
      </InlineField>
    </Stack>
  );
}
