import React from 'react';
import { fireEvent, render, screen } from '@testing-library/react';
import type { QueryEditorProps } from '@grafana/data';

import type { DataSource } from '../datasource';
import type { ParkanaDataSourceOptions, ParkanaQuery } from '../types';
import { QueryEditor } from './QueryEditor';

// Thin, accessible stand-ins for the @grafana/ui components used by the
// editor: native textarea/input/fieldset/label elements wired to the real
// onChange/onBlur handlers. Keeping the component graph out of the test
// runtime means no Grafana UI or runtime services are ever initialized here.
jest.mock('@grafana/ui', () => {
  const React = require('react');

  const TextArea = (props: Record<string, unknown>) => React.createElement('textarea', props);
  const Input = ({ width, ...rest }: Record<string, unknown>) => React.createElement('input', rest);
  const Stack = ({ children }: { children?: React.ReactNode }) => React.createElement('div', null, children);
  const InlineFieldRow = ({ children }: { children?: React.ReactNode }) => React.createElement('div', null, children);
  // Like the real InlineField, associate the label with the control through
  // its id so duplicated ids across query rows are observable in tests.
  const InlineField = ({
    label,
    useFieldset,
    children,
  }: {
    label?: string;
    useFieldset?: boolean;
    children?: React.ReactElement<{ id?: string }>;
  }) => {
    const control = React.Children.only(children);
    const htmlFor = React.isValidElement(control) ? control.props.id : undefined;
    const labelEl = React.createElement('label', { htmlFor }, label);
    if (useFieldset) {
      return React.createElement('fieldset', null, React.createElement('legend', null, label), children);
    }
    return React.createElement('div', null, labelEl, children);
  };
  const RadioButtonGroup = ({
    options,
    value,
    onChange,
    'aria-label': ariaLabel,
  }: {
    options: Array<{ label: string; value: string }>;
    value?: string;
    onChange?: (value: string) => void;
    'aria-label'?: string;
  }) =>
    React.createElement(
      'div',
      { role: 'radiogroup', 'aria-label': ariaLabel },
      options.map((option) =>
        React.createElement(
          'label',
          { key: option.value },
          React.createElement('input', {
            type: 'radio',
            checked: option.value === value,
            onChange: () => onChange?.(option.value),
          }),
          option.label
        )
      )
    );
  const Text = ({ element = 'span', children }: { element?: string; children?: React.ReactNode }) =>
    React.createElement(element, null, children);
  const CollapsableSection = ({
    label,
    isOpen,
    onToggle,
    headerDataTestId,
    contentDataTestId,
    children,
  }: {
    label?: string;
    isOpen: boolean;
    onToggle?: (isOpen: boolean) => void;
    headerDataTestId?: string;
    contentDataTestId?: string;
    children?: React.ReactNode;
  }) =>
    React.createElement(
      'div',
      null,
      React.createElement(
        'button',
        { type: 'button', 'data-testid': headerDataTestId, onClick: () => onToggle?.(!isOpen) },
        label
      ),
      isOpen ? React.createElement('div', { 'data-testid': contentDataTestId }, children) : null
    );

  return { TextArea, Input, Stack, InlineField, InlineFieldRow, RadioButtonGroup, Text, CollapsableSection };
});

// The editor never touches the datasource instance; keep the fixture out of
// Grafana's runtime initialization entirely.
const datasource = {} as DataSource;

function renderEditor(query: Partial<ParkanaQuery>) {
  const onChange = jest.fn();
  const onRunQuery = jest.fn();
  const props: QueryEditorProps<DataSource, ParkanaQuery, ParkanaDataSourceOptions> = {
    query: query as ParkanaQuery,
    onChange,
    onRunQuery,
    datasource,
  };
  render(<QueryEditor {...props} />);
  return { onChange, onRunQuery };
}

describe('QueryEditor', () => {
  it('renders the current SQL', () => {
    renderEditor({ refId: 'A', rawSQL: 'SELECT 1', format: 'table' });
    expect(screen.getByLabelText('SQL query')).toHaveValue('SELECT 1');
  });

  it('applies the table format when the query carries none', () => {
    renderEditor({ refId: 'A', rawSQL: 'SELECT 1' });
    expect(screen.getByRole('radio', { name: 'Table' })).toBeChecked();
  });

  it('edits SQL and preserves format, limit and refId', () => {
    const { onChange } = renderEditor({ refId: 'A', rawSQL: 'SELECT 1', format: 'time_series', limit: 50 });
    fireEvent.change(screen.getByLabelText('SQL query'), { target: { value: 'SELECT 2' } });
    expect(onChange).toHaveBeenCalledWith({ refId: 'A', rawSQL: 'SELECT 2', format: 'time_series', limit: 50 });
  });

  it('switches to the time series format and preserves the SQL', () => {
    const { onChange } = renderEditor({ refId: 'A', rawSQL: 'SELECT _ts, count(*) FROM logs', format: 'table' });
    fireEvent.click(screen.getByRole('radio', { name: 'Time series' }));
    expect(onChange).toHaveBeenCalledWith({
      refId: 'A',
      rawSQL: 'SELECT _ts, count(*) FROM logs',
      format: 'time_series',
    });
  });

  it('switches back to the table format and preserves the SQL', () => {
    const { onChange } = renderEditor({ refId: 'A', rawSQL: 'SELECT _ts, count(*) FROM logs', format: 'time_series' });
    fireEvent.click(screen.getByRole('radio', { name: 'Table' }));
    expect(onChange).toHaveBeenCalledWith({ refId: 'A', rawSQL: 'SELECT _ts, count(*) FROM logs', format: 'table' });
  });

  it('parses the row limit on input', () => {
    const { onChange } = renderEditor({ refId: 'A', rawSQL: 'SELECT 1', format: 'table' });
    fireEvent.change(screen.getByLabelText('Row limit'), { target: { value: '500' } });
    expect(onChange).toHaveBeenCalledWith({ refId: 'A', rawSQL: 'SELECT 1', format: 'table', limit: 500 });
  });

  it('clears the row limit when the input is emptied', () => {
    const { onChange } = renderEditor({ refId: 'A', rawSQL: 'SELECT 1', format: 'table', limit: 500 });
    fireEvent.change(screen.getByLabelText('Row limit'), { target: { value: '' } });
    expect(onChange).toHaveBeenCalledWith({ refId: 'A', rawSQL: 'SELECT 1', format: 'table', limit: undefined });
  });

  it('runs the query on blur of the SQL input', () => {
    const { onRunQuery } = renderEditor({ refId: 'A', rawSQL: 'SELECT 1', format: 'table' });
    fireEvent.blur(screen.getByLabelText('SQL query'));
    expect(onRunQuery).toHaveBeenCalled();
  });

  it('shows the macro help content when expanded', () => {
    renderEditor({ refId: 'A', rawSQL: 'SELECT 1', format: 'table' });
    fireEvent.click(screen.getByTestId('macro-help-header'));
    const content = screen.getByTestId('macro-help-content');
    expect(content).toHaveTextContent('$__timeFrom');
    expect(content).toHaveTextContent('$__timeTo');
    expect(content).toHaveTextContent('$__interval');
  });

  it('gives every query row its own input ids and label associations', () => {
    renderEditor({ refId: 'A', rawSQL: 'SELECT 1', format: 'table' });
    renderEditor({ refId: 'B', rawSQL: 'SELECT 2', format: 'table' });

    const sqlInputs = screen.getAllByLabelText('SQL query');
    const limitInputs = screen.getAllByLabelText('Row limit');
    expect(sqlInputs).toHaveLength(2);
    expect(limitInputs).toHaveLength(2);

    const ids = [...sqlInputs, ...limitInputs].map((element) => element.id);
    expect(new Set(ids).size).toBe(ids.length);
    expect(sqlInputs[0].id).not.toBe(sqlInputs[1].id);
    expect(limitInputs[0].id).not.toBe(limitInputs[1].id);
  });
});
