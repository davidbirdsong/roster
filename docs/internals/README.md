# Internals

For people changing the library. These record decisions, the alternatives turned down, and problems already settled. How to use the library is in [the user docs](../README.md), and where the two differ the user docs and the code are current.

| Document | Holds |
|---|---|
| [design.md](design.md) | The model, its scope, the data contract and open decisions. Read before changing the model |
| [design-s3-hints.md](design-s3-hints.md) | Storage, the wire format rules and change hints. Read before changing any of them |
| [design-notifier.md](design-notifier.md) | Write notifiers and the rules the library keeps |
| [TODO.md](TODO.md) | Open questions and follow ups |
| [request-model.md](request-model.md) | Every call site that reaches S3. Update it with the code |
| [analysis-s3-mqtt-complexity.md](analysis-s3-mqtt-complexity.md) | Cost and benefit of S3 over what came before |
| [analysis-marker-baseline-ordering.md](analysis-marker-baseline-ordering.md) | The lost mark race and its fix |
| [analysis-undecodable-document.md](analysis-undecodable-document.md) | Why `ListOwned` reports damaged objects |
| [analysis-hash-injectivity.md](analysis-hash-injectivity.md) | Why the content hash is length prefixed |
| [registry-review.md](registry-review.md) | An outside review from before the rename, kept as written |
