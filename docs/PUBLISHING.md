# Publishing

## Package build

```bash
npm install
npm run build:all
npm test
```

Artifacts are emitted to `dist/`:
- ESM
- CommonJS
- declaration files
- source maps

## npm publish

Required secret:
- `NPM_TOKEN`

Local publish:

```bash
npm publish --access public
```

GitHub Actions publish:
- use the `Publish to npm` workflow
- or create a GitHub release to trigger it

## GitHub Packages publish

The repository includes a separate workflow for GitHub Packages.
That keeps npmjs.org and GitHub Packages paths independent.

## Notes

- `prepack` runs `build:all`, so packed artifacts stay consistent.
- `files` in `package.json` only publishes `dist`, docs, README, and LICENSE.
- keep version bumps explicit and tag releases cleanly.


## Before first public publish

Set these fields in `package.json` to the real repository values for your repo:
- `repository`
- `bugs`
- `homepage`
