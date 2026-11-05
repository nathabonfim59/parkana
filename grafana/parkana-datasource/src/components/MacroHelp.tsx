import React from 'react';
import { CollapsableSection, Stack, Text } from '@grafana/ui';
import { css } from '@emotion/css';

import { MACROS } from '../macros';

interface Props {
  isOpen: boolean;
  onToggle: (isOpen: boolean) => void;
}

const helpStyles = css`
  pre {
    margin: 4px 0 0;
    padding: 8px;
    white-space: pre-wrap;
  }
`;

/**
 * Static help for the SQL macros listed in `../macros`. This is
 * documentation only: macro expansion happens in the plugin backend.
 */
export function MacroHelp({ isOpen, onToggle }: Props) {
  return (
    <CollapsableSection
      label="SQL macros"
      isOpen={isOpen}
      onToggle={onToggle}
      contentDataTestId="macro-help-content"
      headerDataTestId="macro-help-header"
    >
      <div className={helpStyles}>
        <Text element="p" color="secondary">
          Macros are substituted by the Parkana backend into valid DuckDB literals, outside SQL strings and comments.
        </Text>
        <Stack gap={2} direction="column">
          {MACROS.map((macro) => (
            <div key={macro.name} data-testid={`macro-help-${macro.name}`}>
              {macro.signatures.map((signature) => (
                <div key={signature}>
                  <code>{signature}</code>
                </div>
              ))}
              <Text element="p" color="secondary">
                {macro.description}
              </Text>
              <pre>
                <code>{macro.example}</code>
              </pre>
            </div>
          ))}
        </Stack>
      </div>
    </CollapsableSection>
  );
}
