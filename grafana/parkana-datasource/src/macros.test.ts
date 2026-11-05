import { MACROS } from './macros';

describe('macro help data', () => {
  it('documents the backend-supported macros', () => {
    expect(MACROS.map((macro) => macro.name)).toEqual(['timeFrom', 'timeTo', 'interval']);
  });

  it('lists the bare spelling for every macro', () => {
    for (const macro of MACROS) {
      expect(macro.signatures).toContain(`$__${macro.name}`);
    }
  });

  it('documents the function form where the backend accepts it', () => {
    const byName = new Map(MACROS.map((macro) => [macro.name, macro]));
    expect(byName.get('timeFrom')?.signatures).toContain('$__timeFrom()');
    expect(byName.get('timeTo')?.signatures).toContain('$__timeTo()');
    expect(byName.get('interval')?.signatures).toContain('$__interval()');
  });

  it('describes every macro and shows an example using its first spelling', () => {
    for (const macro of MACROS) {
      expect(macro.description).not.toBe('');
      expect(macro.example).toContain(macro.signatures[0]);
    }
  });

  it('builds the interval example around time bucketing with a time alias', () => {
    const interval = MACROS.find((macro) => macro.name === 'interval');
    expect(interval?.example).toContain('time_bucket($__interval, _ts)');
    expect(interval?.example).toContain('AS time');
  });
});
