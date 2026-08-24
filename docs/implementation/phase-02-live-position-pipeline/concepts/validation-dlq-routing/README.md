# Validation and DLQ Routing

Concept notes and debrief for validation and dead-letter-queue routing.

| File | What's inside |
| --- | --- |
| [rejection-classification.md](rejection-classification.md) | How the consumer classifies bad records and why the three outcomes are distinct |
| [dlq-design.md](dlq-design.md) | Dead-letter queue purpose, envelope schema, and failure isolation guarantee |
| [offset-commit-contract.md](offset-commit-contract.md) | Why the offset commit is unconditional and what that means for replay |
| [validation-dlq-debrief.md](validation-dlq-debrief.md) | Observed outputs from running all four failure experiments |
