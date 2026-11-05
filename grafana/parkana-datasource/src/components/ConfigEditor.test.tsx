import React from 'react';
import { fireEvent, render, screen } from '@testing-library/react';
import type { DataSourcePluginOptionsEditorProps } from '@grafana/data';

import type { ParkanaDataSourceOptions } from '../types';
import { ConfigEditor } from './ConfigEditor';

// Thin native-element stand-ins for the @grafana/ui components the config
// editor uses, wired to the real onChange handlers. No Grafana UI or runtime
// services are loaded by this suite.
jest.mock('@grafana/ui', () => {
  const React = require('react');

  const Input = ({ width, ...rest }: Record<string, unknown>) => React.createElement('input', rest);
  const Stack = ({ children }: { children?: React.ReactNode }) => React.createElement('div', null, children);
  const InlineField = ({ label, children }: { label?: string; children?: React.ReactNode }) =>
    React.createElement('label', null, React.createElement('span', null, label), children);

  return { Input, Stack, InlineField };
});

function renderEditor(jsonData: Partial<ParkanaDataSourceOptions>) {
  const onChange = jest.fn();
  const options = {
    jsonData: jsonData as ParkanaDataSourceOptions,
  } as DataSourcePluginOptionsEditorProps<ParkanaDataSourceOptions>['options'];
  render(<ConfigEditor onOptionsChange={onChange} options={options} />);
  return { onChange, options };
}

describe('ConfigEditor', () => {
  it('renders the configured URL and tenant', () => {
    renderEditor({ url: 'http://parkana:3100', tenant: 'acme' });
    expect(screen.getByLabelText(/^URL/)).toHaveValue('http://parkana:3100');
    expect(screen.getByLabelText(/^Tenant/)).toHaveValue('acme');
  });

  it('updates the URL and preserves the tenant', () => {
    const { onChange } = renderEditor({ url: 'http://parkana:3100', tenant: 'acme' });
    fireEvent.change(screen.getByLabelText(/^URL/), { target: { value: 'http://parkana:3101' } });
    expect(onChange).toHaveBeenCalledWith(
      expect.objectContaining({
        jsonData: expect.objectContaining({ url: 'http://parkana:3101', tenant: 'acme' }),
      })
    );
  });

  it('updates the tenant and preserves the URL', () => {
    const { onChange } = renderEditor({ url: 'http://parkana:3100', tenant: 'acme' });
    fireEvent.change(screen.getByLabelText(/^Tenant/), { target: { value: 'other' } });
    expect(onChange).toHaveBeenCalledWith(
      expect.objectContaining({
        jsonData: expect.objectContaining({ url: 'http://parkana:3100', tenant: 'other' }),
      })
    );
  });

  it('renders empty fields for missing jsonData values', () => {
    renderEditor({ url: '' });
    expect(screen.getByLabelText(/^URL/)).toHaveValue('');
    expect(screen.getByLabelText(/^Tenant/)).toHaveValue('');
  });
});
