# Firebase Data Connect Notes

Firebase Data Connect is operation-driven, not runtime-discovered.
That means the clean approach is:

1. define your collection contract in this package
2. generate Data Connect operations in your Firebase project
3. map those generated operations through `createDataConnectAdapter()`
4. let `SqlShardProvider` handle validation, normalization, includes, and store helpers

## Recommended split

Use this package for:
- validation
- provider orchestration
- relation metadata
- store/page state helpers

Use Firebase Data Connect for:
- SQL execution
- schema evolution
- generated operations
- secure backend access patterns

## Practical advice

Do not try to make Data Connect behave like Firestore.
Treat it as a typed operation layer and keep your adapter explicit.
