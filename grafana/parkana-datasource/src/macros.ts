/**
 * Frontend-only documentation of the SQL macros the Parkana backend expands.
 * The frontend never expands macros itself: the query text reaches the
 * backend unchanged, so this data only drives the editor help panel.
 */

export interface MacroDoc {
  /** Macro name without the leading `$__`. */
  name: string;
  /** The spellings the backend accepts, as written in SQL. */
  signatures: string[];
  /** What the backend substitutes for the macro. */
  description: string;
  /** A SQL fragment using the macro. */
  example: string;
}

/** Macros supported by the Parkana SQL backend, in help-panel order. */
export const MACROS: MacroDoc[] = [
  {
    name: 'timeFrom',
    signatures: ['$__timeFrom', '$__timeFrom()'],
    description: 'Start of the dashboard time range (inclusive) as a DuckDB timestamp literal.',
    example: 'WHERE _ts >= $__timeFrom',
  },
  {
    name: 'timeTo',
    signatures: ['$__timeTo', '$__timeTo()'],
    description: 'End of the dashboard time range (exclusive) as a DuckDB timestamp literal.',
    example: 'WHERE _ts < $__timeTo',
  },
  {
    name: 'interval',
    signatures: ['$__interval', '$__interval()'],
    description: 'Panel grouping interval as a DuckDB interval literal.',
    example: 'time_bucket($__interval, _ts) AS time',
  },
];
