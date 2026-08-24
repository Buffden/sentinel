# CP4: Validation and DLQ Routing

Concept notes and debrief for the validation and dead-letter-queue checkpoint.

| File | What's inside |
| --- | --- |
| [rejection-classification.md](rejection-classification.md) | How the consumer classifies bad records and why the three outcomes are distinct |
| [dlq-design.md](dlq-design.md) | Dead-letter queue purpose, envelope schema, and failure isolation guarantee |
| [offset-commit-contract.md](offset-commit-contract.md) | Why the offset commit is unconditional and what that means for replay |
| [cp4-debrief.md](cp4-debrief.md) | Observed outputs from running all four failure experiments |
