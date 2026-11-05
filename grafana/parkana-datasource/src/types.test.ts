import { DEFAULT_FORMAT, DEFAULT_QUERY, formatLimitInput, parseLimitInput } from './types';

describe('Parkana query model', () => {
  describe('DEFAULT_QUERY', () => {
    it('defaults to the table format', () => {
      expect(DEFAULT_QUERY.format).toBe(DEFAULT_FORMAT);
      expect(DEFAULT_FORMAT).toBe('table');
    });

    it('seeds SQL that demonstrates the time range macros', () => {
      expect(DEFAULT_QUERY.rawSQL).toContain('$__timeFrom');
      expect(DEFAULT_QUERY.rawSQL).toContain('$__timeTo');
    });

    it('does not set a row limit, so the server default applies', () => {
      expect(DEFAULT_QUERY.limit).toBeUndefined();
    });
  });

  describe('parseLimitInput', () => {
    it.each([
      ['', undefined],
      ['   ', undefined],
      [undefined, undefined],
      ['0', undefined],
      ['-5', undefined],
      ['abc', undefined],
      ['NaN', undefined],
      ['Infinity', undefined],
      ['2.9', undefined],
      ['0.5', undefined],
      ['1e30', undefined],
      ['9007199254740992', undefined],
      ['2.0', 2],
      ['1', 1],
      ['10', 10],
      [' 10 ', 10],
      ['9007199254740991', 9007199254740991],
    ])('parses %s to %p', (input, expected) => {
      expect(parseLimitInput(input as string | undefined)).toBe(expected);
    });
  });

  describe('formatLimitInput', () => {
    it('renders undefined as the empty string', () => {
      expect(formatLimitInput(undefined)).toBe('');
    });

    it('renders a limit as its decimal text', () => {
      expect(formatLimitInput(10)).toBe('10');
    });
  });

  it('round-trips limits through parse and format', () => {
    expect(formatLimitInput(parseLimitInput('250'))).toBe('250');
    expect(parseLimitInput(formatLimitInput(250))).toBe(250);
  });
});
