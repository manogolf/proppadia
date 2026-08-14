# Minimum future prediction provenance invariant

Every frozen prediction must contain, or immutably reference: model semantic ID, exact model SHA-256, feature-contract SHA-256, producer/run tag, and prediction timestamp. The freeze operation must reject a row if those identifiers are absent. Historical rows are not altered by this recommendation.
