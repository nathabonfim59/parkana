import type { LanguageRegistration } from 'shiki'

// A small TextMate grammar for LogQL code fences (```logql), which Shiki does not ship. It colors
// what a reader scans for: stream selectors and their matchers, line filters, parser and
// formatting stages, range and vector functions, grouping, durations, strings and numbers.
export const logql: LanguageRegistration = {
  name: 'logql',
  scopeName: 'source.logql',
  patterns: [
    { include: '#comment' },
    { include: '#selector' },
    { include: '#string' },
    { include: '#range' },
    { include: '#filter' },
    { include: '#stage' },
    { include: '#function' },
    { include: '#keyword' },
    { include: '#number' },
    { include: '#operator' },
    { include: '#variable' },
  ],
  repository: {
    comment: { name: 'comment.line.number-sign.logql', match: '#.*$' },
    selector: {
      name: 'meta.selector.logql',
      begin: '\\{',
      end: '\\}',
      beginCaptures: { 0: { name: 'punctuation.definition.selector.begin.logql' } },
      endCaptures: { 0: { name: 'punctuation.definition.selector.end.logql' } },
      patterns: [
        { name: 'entity.name.tag.label.logql', match: '[A-Za-z_][A-Za-z0-9_]*(?=\\s*(=~|!~|!=|=))' },
        { name: 'keyword.operator.matcher.logql', match: '=~|!~|!=|=' },
        { include: '#string' },
        { name: 'punctuation.separator.logql', match: ',' },
      ],
    },
    string: {
      patterns: [
        {
          name: 'string.quoted.double.logql',
          begin: '"',
          end: '"',
          patterns: [{ name: 'constant.character.escape.logql', match: '\\\\.' }],
        },
        { name: 'string.quoted.other.raw.logql', begin: '`', end: '`' },
      ],
    },
    range: {
      name: 'meta.range.logql',
      match: '(\\[)\\s*([^\\]]+?)\\s*(\\])',
      captures: {
        1: { name: 'punctuation.definition.range.begin.logql' },
        2: { name: 'constant.other.duration.logql' },
        3: { name: 'punctuation.definition.range.end.logql' },
      },
    },
    filter: { name: 'keyword.operator.filter.logql', match: '\\|=|!=|\\|~|!~|\\|>|!>' },
    stage: {
      match: '(\\|)\\s*(json|logfmt|regexp|pattern|unpack|line_format|label_format|labels_format|drop|keep|unwrap|decolorize|ip)\\b',
      captures: {
        1: { name: 'keyword.operator.pipe.logql' },
        2: { name: 'support.function.stage.logql' },
      },
    },
    function: {
      name: 'support.function.logql',
      match:
        '\\b(count_over_time|rate|rate_counter|bytes_over_time|bytes_rate|absent_over_time|avg_over_time|sum_over_time|min_over_time|max_over_time|stddev_over_time|stdvar_over_time|quantile_over_time|first_over_time|last_over_time|sum|avg|min|max|count|stddev|stdvar|topk|bottomk|sort|sort_desc|label_replace|vector|bytes|duration|duration_seconds)(?=\\s*\\()',
    },
    keyword: { name: 'keyword.control.logql', match: '\\b(by|without|offset|bool|on|ignoring|group_left|group_right|and|or|unless)\\b' },
    number: {
      patterns: [
        { name: 'constant.numeric.duration.logql', match: '\\b[0-9]+(?:\\.[0-9]+)?(?:ns|us|µs|ms|s|m|h|d|w|y)\\b' },
        { name: 'constant.numeric.logql', match: '\\b[0-9]+(?:\\.[0-9]+)?\\b' },
      ],
    },
    operator: { name: 'keyword.operator.logql', match: '==|!=|>=|<=|>|<|\\+|-|\\*|/|%|\\^|\\|' },
    variable: { name: 'variable.other.template.logql', match: '\\$\\{?__?[A-Za-z_]+\\}?' },
  },
}
