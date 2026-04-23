import { compileCollectionSchema } from '../core/schemaCompiler.js';

export class CollectionRuntime {
  constructor(definition) {
    this.definition = definition;
    this.schema = compileCollectionSchema(definition);
  }

  validateCreate(data, options) {
    return this.schema.validateCreate(data, options);
  }

  validateUpdate(data, existingDoc, options) {
    return this.schema.validateUpdate(data, existingDoc, options);
  }
}
