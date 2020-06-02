module.exports = {
  clearMocks: true,
  preset: 'ts-jest',
  testMatch: ['**/*test.(js|ts)'],
  testPathIgnorePatterns: ['<rootDir>/dist/', '<rootDir>/node_modules/'],
  testEnvironment: 'node',
  collectCoverage: true,
  collectCoverageFrom: ['src/**/*.ts'],
  globals: {
    'ts-jest': {
      isolatedModules: true,
    },
  },
};
