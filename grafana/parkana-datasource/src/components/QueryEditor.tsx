import React, { ChangeEvent, useId, useState } from 'react';
import { InlineField, InlineFieldRow, Input, RadioButtonGroup, Stack, TextArea } from '@grafana/ui';
import type { QueryEditorProps, SelectableValue } from '@grafana/data';

import type { DataSource } from '../datasource';
import {
  DEFAULT_FORMAT,
  formatLimitInput,
  parseLimitInput,
  type ParkanaDataSourceOptions,
  type ParkanaQuery,
  type QueryFormat,
} from '../types';
import { MacroHelp } from './MacroHelp';

type Props = QueryEditorProps<DataSource, ParkanaQuery, ParkanaDataSourceOptions>;

const FORMAT_OPTIONS: Array<SelectableValue<QueryFormat>> = [
  { label: 'Table', value: 'table', description: 'One row per SQL result row.' },
  {
    label: 'Time series',
    value: 'time_series',
    description: 'Requires a "time" timestamp column plus numeric columns.',
  },
];

const LIMIT_TOOLTIP = 'Maximum rows returned before the result is marked truncated. Empty uses the server default.';

export function QueryEditor({ query, onChange, onRunQuery }: Props) {
  const format = query.format ?? DEFAULT_FORMAT;
  const [macroHelpOpen, setMacroHelpOpen] = useState(false);
  // Several query rows can be open at once; static ids would make labels
  // point at the first row's inputs.
  const sqlId = useId();
  const limitId = useId();

  const onSQLChange = (event: ChangeEvent<HTMLTextAreaElement>) => {
    onChange({ ...query, rawSQL: event.currentTarget.value });
  };

  const onFormatChange = (value: QueryFormat) => {
    onChange({ ...query, format: value });
  };

  const onLimitChange = (event: ChangeEvent<HTMLInputElement>) => {
    onChange({ ...query, limit: parseLimitInput(event.currentTarget.value) });
  };

  return (
    <Stack gap={1} direction="column">
      <InlineField
        label="SQL query"
        labelWidth={14}
        tooltip="DuckDB SQL against Parkana logs. $__-macros are expanded by the backend."
        grow
      >
        <TextArea
          id={sqlId}
          onChange={onSQLChange}
          onBlur={onRunQuery}
          value={query.rawSQL ?? ''}
          rows={6}
          placeholder="SELECT _ts, line FROM logs WHERE _ts >= $__timeFrom AND _ts < $__timeTo"
          spellCheck={false}
        />
      </InlineField>
      <InlineFieldRow>
        <InlineField label="Format" labelWidth={10} tooltip="Result shape requested from Parkana." useFieldset>
          <RadioButtonGroup<QueryFormat>
            aria-label="Format"
            options={FORMAT_OPTIONS}
            value={format}
            onChange={onFormatChange}
          />
        </InlineField>
        <InlineField label="Row limit" labelWidth={14} tooltip={LIMIT_TOOLTIP}>
          <Input
            id={limitId}
            type="number"
            min={1}
            step={1}
            placeholder="Server default"
            onChange={onLimitChange}
            onBlur={onRunQuery}
            value={formatLimitInput(query.limit)}
            width={16}
          />
        </InlineField>
      </InlineFieldRow>
      <MacroHelp isOpen={macroHelpOpen} onToggle={setMacroHelpOpen} />
    </Stack>
  );
}
