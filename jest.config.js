/* eslint-disable */

const {
  buildJestConfig,
} = require('@jupiterone/typescript-tools/config/jest-util');
module.exports = {
  ...buildJestConfig({ packageDir: __dirname }),
  preset: 'ts-jest',
  // The below is necessary due to differences between how p-map is packaged and our
  // generic J1 babel and TS configs
  transformIgnorePatterns: ['../../node_modules/(?!${p-map})'],
};
/* eslint-enable */
