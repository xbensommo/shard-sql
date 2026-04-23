export default {
  entry: {
    index: 'src/index.js',
    actions: 'src/actions/createShardedActions.js',
    adapters: 'src/adapters/createSqlAdapter.js',
    'adapters/dataconnect': 'src/adapters/createDataConnectAdapter.js',
    runtime: 'src/runtime/SqlShardProvider.js',
    validators: 'src/validators.js',
  },
  format: ['esm', 'cjs'],
  target: 'node18',
  platform: 'node',
  sourcemap: true,
  clean: false,
  splitting: false,
  bundle: true,
  treeshake: true,
  outDir: 'dist',
  outExtension({ format }) {
    return {
      js: format === 'cjs' ? '.cjs' : '.js',
    };
  },
};
